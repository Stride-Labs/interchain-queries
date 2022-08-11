package runner

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Stride-Labs/interchain-queries/pkg/config"
	qstypes "github.com/Stride-Labs/stride/x/interchainquery/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
	lensclient "github.com/strangelove-ventures/lens/client"
	lensquery "github.com/strangelove-ventures/lens/client/query"
	abcitypes "github.com/tendermint/tendermint/abci/types"
	tmquery "github.com/tendermint/tendermint/libs/pubsub/query"
	coretypes "github.com/tendermint/tendermint/rpc/core/types"
	"google.golang.org/grpc/metadata"

	clienttypes "github.com/cosmos/ibc-go/v3/modules/core/02-client/types"
	tmclient "github.com/cosmos/ibc-go/v3/modules/light-clients/07-tendermint/types"
	tmproto "github.com/tendermint/tendermint/proto/tendermint/types"
	tmtypes "github.com/tendermint/tendermint/types"
)

type Clients []*lensclient.ChainClient

var (
	WaitInterval = time.Second * 3
	clients      = Clients{}
	ctx          = context.Background()
	sendQueue    = map[string]chan sdk.Msg{}
)

func (clients Clients) GetForChainId(chainId string) *lensclient.ChainClient {
	for _, client := range clients {
		if client.Config.ChainID == chainId {
			return client
		}
	}
	return nil
}

func Run(cfg *config.Config, home string) error {
	defer Close()
	for _, c := range cfg.Chains {
		client, err := lensclient.NewChainClient(nil, c, home, os.Stdin, os.Stdout)
		if err != nil {
			return err
		}
		sendQueue[client.Config.ChainID] = make(chan sdk.Msg)
		clients = append(clients, client)
	}

	query := tmquery.MustParse(fmt.Sprintf("message.module='%s'", "interchainquery"))

	wg := &sync.WaitGroup{}

	for _, client := range clients {
		err := client.RPCClient.Start()
		if err != nil {
			fmt.Println(err)
		}

		ch, err := client.RPCClient.Subscribe(ctx, client.Config.ChainID+"-icq", query.String())
		if err != nil {
			fmt.Println(err)
			return err
		}

		wg.Add(1)
		go func(chainId string, ch <-chan coretypes.ResultEvent) {
			for v := range ch {
				v.Events["source"] = []string{chainId}
				go handleEvent(v)
			}
		}(client.Config.ChainID, ch)
	}

	for _, client := range clients {
		wg.Add(1)
		go FlushSendQueue(client.Config.ChainID)
	}

	wg.Wait()
	return nil
}

type Query struct {
	SourceChainId string
	ConnectionId  string
	ChainId       string
	QueryId       string
	Type          string
	Height        int64
	Request       []byte
}

func handleEvent(event coretypes.ResultEvent) {
	queries := []Query{}
	source := event.Events["source"]
	connections := event.Events["message.connection_id"]
	chains := event.Events["message.chain_id"]
	queryIds := event.Events["message.query_id"]
	types := event.Events["message.type"]
	request := event.Events["message.request"]
	height := event.Events["message.height"]

	items := len(queryIds)

	for i := 0; i < items; i++ {
		req, err := hex.DecodeString(request[i])
		if err != nil {
			panic(err)
		}
		h, err := strconv.ParseInt(height[i], 10, 64)
		if err != nil {
			panic(err)
		}
		queries = append(queries, Query{source[0], connections[i], chains[i], queryIds[i], types[i], h, req})
	}

	for _, q := range queries {
		go doRequest(q)
	}
}

func RunGRPCQuery(ctx context.Context, client *lensclient.ChainClient, method string, reqBz []byte, md metadata.MD) (abcitypes.ResponseQuery, metadata.MD, error) {
	// parse height header
	height, err := lensclient.GetHeightFromMetadata(md)
	if err != nil {
		return abcitypes.ResponseQuery{}, nil, err
	}
	fmt.Println("height parsed from GetHeightFromMetadata=", height)

	prove, err := lensclient.GetProveFromMetadata(md)
	if err != nil {
		return abcitypes.ResponseQuery{}, nil, err
	}

	abciReq := abcitypes.RequestQuery{
		Path:   method,
		Data:   reqBz,
		Height: height,
		Prove:  prove,
	}

	abciRes, err := client.QueryABCI(ctx, abciReq)
	if err != nil {
		return abcitypes.ResponseQuery{}, nil, err
	}

	return abciRes, md, nil
}

func retryLightblock(ctx context.Context, client *lensclient.ChainClient, height int64, maxTime int) (*tmtypes.LightBlock, error) {
	interval := 1
	lightBlock, err := client.LightProvider.LightBlock(ctx, height)
	if err != nil {
		for {
			time.Sleep(time.Duration(interval) * time.Second)
			fmt.Println("Requerying lightblock")
			lightBlock, err = client.LightProvider.LightBlock(ctx, height)
			interval = interval + 1
			if err == nil {
				break
			} else if interval > maxTime {
				return nil, fmt.Errorf("unable to query light block, max interval exceeded")
			}
		}
	}
	return lightBlock, err
}

func doRequest(query Query) {
	client := clients.GetForChainId(query.ChainId)
	if client == nil {
		fmt.Println("No chain")
		return
	}
	fmt.Println(query.Type)
	newCtx := lensclient.SetHeightOnContext(ctx, query.Height)
	pathParts := strings.Split(query.Type, "/")
	if pathParts[len(pathParts)-1] == "key" { // fetch proof if the query is 'key'
		newCtx = lensclient.SetProveOnContext(newCtx, true)
	}
	inMd, ok := metadata.FromOutgoingContext(newCtx)
	if !ok {
		panic("failed on not ok")
	}

	res, _, err := RunGRPCQuery(ctx, client, "/"+query.Type, query.Request, inMd)
	if err != nil {
		panic(err)
	}
	// get latest client chain block height to use in LC update and proof
	// abciInfo, _ := client.RPCClient.ABCIInfo(ctx)
	// lastBlockHeight := abciInfo.Response.LastBlockHeight
	// fmt.Println("Latest block height on Gaia from ABCI: ", lastBlockHeight)

	// submit tx to queue
	submitClient := clients.GetForChainId(query.SourceChainId)
	from, _ := submitClient.GetKeyAddress()
	if pathParts[len(pathParts)-1] == "key" {

		submitQuerier := lensquery.Query{Client: submitClient, Options: lensquery.DefaultOptions()}
		connection, err := submitQuerier.Ibc_Connection(query.ConnectionId)
		if err != nil {
			fmt.Println("Error: Could not get connection from chain: ", err)
			panic("Error: Could not fetch updated LC from chain - bailing")
		}

		clientId := connection.Connection.ClientId
		state, err := submitQuerier.Ibc_ClientState(clientId) // pass in from request
		if err != nil {
			fmt.Println("Error: Could not get state from chain: ", err)
			panic("Could not get state from chain")
		}
		unpackedState, err := clienttypes.UnpackClientState(state.ClientState)
		if err != nil {
			fmt.Println("Error: Could not unpack state from chain: ", err)
			panic("Could not unpack state from chain")
		}

		trustedHeight := unpackedState.GetLatestHeight()
		clientHeight := trustedHeight.(clienttypes.Height)
		if !ok {
			fmt.Println("Error: Could not coerce trusted height")
			panic("Error: Could coerce trusted height")
		}

		consensus, err := submitQuerier.Ibc_ConsensusState(clientId, clientHeight) // pass in from request
		if err != nil {
			fmt.Println("Error: Could not get consensus state from chain: ", err)
			panic("Error: Could not get consensus state from chain: ")
		}
		unpackedConsensus, err := clienttypes.UnpackConsensusState(consensus.ConsensusState)
		if err != nil {
			fmt.Println("Error: Could not unpack consensus state from chain: ", err)
			panic("Error: Could not unpack consensus state from chain: ")
		}

		tmConsensus := unpackedConsensus.(*tmclient.ConsensusState)

		//------------------------------------------------------

		// update client
		fmt.Println("Fetching client update for height", "height", res.Height+1)
		lightBlock, err := retryLightblock(ctx, client, res.Height+1, 5)
		if err != nil {
			fmt.Println("Error: Could not fetch updated LC from chain - bailing: ", err) // requeue
			return
		}
		valSet := tmtypes.NewValidatorSet(lightBlock.ValidatorSet.Validators)
		protoVal, err := valSet.ToProto()
		if err != nil {
			fmt.Println("Error: Could not get valset from chain: ", err)
			return
		}

		var trustedValset *tmproto.ValidatorSet
		if bytes.Equal(valSet.Hash(), tmConsensus.NextValidatorsHash) {
			trustedValset = protoVal
		} else {
			fmt.Println("Fetching client update for height", "height", res.Height+1)
			lightBlock2, err := retryLightblock(ctx, client, int64(res.Height), 5)
			if err != nil {
				fmt.Println("Error: Could not fetch updated LC2 from chain - bailing: ", err) // requeue
				panic("Error: Could not fetch updated LC2 from chain - bailing: ")
			}
			valSet := tmtypes.NewValidatorSet(lightBlock2.ValidatorSet.Validators)
			trustedValset, err = valSet.ToProto()
			if err != nil {
				fmt.Println("Error: Could not get valset2 from chain: ", err)
				panic("Error: Could not get valset2 from chain: ")
			}
		}

		header := &tmclient.Header{
			SignedHeader:      lightBlock.SignedHeader.ToProto(),
			ValidatorSet:      protoVal,
			TrustedHeight:     clientHeight,
			TrustedValidators: trustedValset,
		}

		anyHeader, err := clienttypes.PackHeader(header)
		if err != nil {
			fmt.Println("Error: Could not get pack header: ", err)
			panic("Error: Could not get pack header: ")
		}

		msg := &clienttypes.MsgUpdateClient{
			ClientId: clientId, // needs to be passed in as part of request.
			Header:   anyHeader,
			Signer:   submitClient.MustEncodeAccAddr(from),
		}

		sendQueue[query.SourceChainId] <- msg

	}

	fmt.Println("ICQ RELAYER | query.Height=", query.Height)
	fmt.Println("ICQ RELAYER | res.Height=", res.Height)
	msg := &qstypes.MsgSubmitQueryResponse{ChainId: query.ChainId, QueryId: query.QueryId, Result: res.Value, ProofOps: res.ProofOps, Height: res.Height, FromAddress: submitClient.MustEncodeAccAddr(from)}
	sendQueue[query.SourceChainId] <- msg
}

func FlushSendQueue(chainId string) {
	time.Sleep(WaitInterval)
	toSend := []sdk.Msg{}
	ch := sendQueue[chainId]

	for {
		if len(toSend) > 12 {
			flush(chainId, toSend)
			toSend = []sdk.Msg{}
		}
		select {
		case msg := <-ch:
			toSend = append(toSend, msg)
		case <-time.After(time.Millisecond * 800):
			flush(chainId, toSend)
			toSend = []sdk.Msg{}
		}
	}
}

func flush(chainId string, toSend []sdk.Msg) {
	if len(toSend) > 0 {
		fmt.Printf("Send batch of %d messages\n", len(toSend))
		client := clients.GetForChainId(chainId)
		if client == nil {
			fmt.Println("No chain")
			return
		}
		// dedupe on queryId
		msgs := unique(toSend)
		resp, err := client.SendMsgs(context.Background(), msgs)
		if err != nil {
			if resp != nil && resp.Code == 19 && resp.Codespace == "sdk" {
				//if err.Error() == "transaction failed with code: 19" {
				fmt.Println("Tx in mempool")
			} else {
				panic(err)
			}
		}
		fmt.Printf("Sent batch of %d (deduplicated) messages\n", len(unique(toSend)))
		// zero messages

	}
}

func unique(msgSlice []sdk.Msg) []sdk.Msg {
	keys := make(map[string]bool)
	clientUpdateHeights := make(map[string]bool)

	list := []sdk.Msg{}
	for _, entry := range msgSlice {
		msg, ok := entry.(*clienttypes.MsgUpdateClient)
		if ok {
			header, _ := clienttypes.UnpackHeader(msg.Header)
			key := header.GetHeight().String()
			if _, value := clientUpdateHeights[key]; !value {
				clientUpdateHeights[key] = true
				list = append(list, entry)
				fmt.Println("1 ClientUpdate message")
			}
			continue
		}
		msg2, ok2 := entry.(*qstypes.MsgSubmitQueryResponse)
		if ok2 {
			if _, value := keys[msg2.QueryId]; !value {
				keys[msg2.QueryId] = true
				list = append(list, entry)
				fmt.Println("1 SubmitResponse message")
			}
		}
	}
	return list
}

func Close() error {

	query := tmquery.MustParse(fmt.Sprintf("message.module='%s'", "interchainquery"))

	for _, client := range clients {
		err := client.RPCClient.Unsubscribe(ctx, client.Config.ChainID+"-icq", query.String())
		if err != nil {
			return err
		}
	}
	return nil
}
