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

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/ingenuity-build/interchain-queries/pkg/config"
	qstypes "github.com/ingenuity-build/quicksilver/x/interchainquery/types"
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
	WaitInterval = time.Second * 2
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

	//fmt.Println("query", "query", abciReq)

	abciRes, err := client.QueryABCI(ctx, abciReq)
	//fmt.Println(abciRes)
	if err != nil {
		return abcitypes.ResponseQuery{}, nil, err
	}

	return abciRes, md, nil
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
	fmt.Println("ctx", "ctx", ctx, "md", inMd)
	if !ok {
		panic("failed on not ok")
	}

	res, _, err := RunGRPCQuery(ctx, client, "/"+query.Type, query.Request, inMd)
	if err != nil {
		panic(err)
	}

	// submit tx to queue
	submitClient := clients.GetForChainId(query.SourceChainId)
	from, _ := submitClient.GetKeyAddress()
	fmt.Println("Result received", "result", res.Value, "proof", res.ProofOps)

	if pathParts[len(pathParts)-1] == "key" {
		// update client
		fmt.Println("Fetching client update for height", "height", res.Height+1)
		lightBlock, err := client.LightProvider.LightBlock(ctx, res.Height+1)
		if err != nil {
			fmt.Println("Error: Could not fetch updated LC from chain: ", err)   // requeue
			return
		}
		valSet := tmtypes.NewValidatorSet(lightBlock.ValidatorSet.Validators)
		protoVal, err := valSet.ToProto()
		if err != nil {
			fmt.Println("Error: Could not get valset from chain: ", err)
			return
		}

		submitQuerier := lensquery.Query{Client: submitClient, Options: lensquery.DefaultOptions()}
		state, _ := submitQuerier.Ibc_ClientState("07-tendermint-0")  // pass in from request
		unpackedState, _ := clienttypes.UnpackClientState(state.ClientState)

		trustedHeight := unpackedState.GetLatestHeight()
		clientHeight, _ := trustedHeight.(clienttypes.Height)

		consensus, _ := submitQuerier.Ibc_ConsensusState("07-tendermint-0", clientHeight)  // pass in from request
		unpackedConsensus, _ := clienttypes.UnpackConsensusState(consensus.ConsensusState)

		//tmClientState := unpackedState.(*tmclient.ClientState)
		tmConsensus := unpackedConsensus.(*tmclient.ConsensusState)

		var trustedValset *tmproto.ValidatorSet
		if bytes.Equal(valSet.Hash(), tmConsensus.NextValidatorsHash) {
			trustedValset = protoVal
		} else {
			panic("trust no-one")  // handle mismatching valsets
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
			return
		}

		msg := &clienttypes.MsgUpdateClient{
			ClientId: "07-tendermint-0", // needs to be passed in as part of request.
			Header:   anyHeader,
			Signer:   submitClient.MustEncodeAccAddr(from),
		}

		sendQueue[query.SourceChainId] <- msg

	}

	msg := &qstypes.MsgSubmitQueryResponse{ChainId: query.ChainId, QueryId: query.QueryId, Result: res.Value, Height: res.Height, ProofOps: res.ProofOps, FromAddress: submitClient.MustEncodeAccAddr(from)}
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
