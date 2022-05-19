package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/ingenuity-build/interchain-queries/pkg/config"
	qstypes "github.com/ingenuity-build/quicksilver/x/interchainquery/types"
	lensclient "github.com/strangelove-ventures/lens/client"
	"github.com/strangelove-ventures/lens/client/staking"
	tmquery "github.com/tendermint/tendermint/libs/pubsub/query"
	coretypes "github.com/tendermint/tendermint/rpc/core/types"
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
		client, err := lensclient.NewChainClient(c, home, os.Stdin, os.Stdout)
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
	Params        map[string]string
}

func handleEvent(event coretypes.ResultEvent) {
	queries := []Query{}
	source := event.Events["source"]
	connections := event.Events["message.connection_id"]
	chains := event.Events["message.chain_id"]
	queryIds := event.Events["message.query_id"]
	types := event.Events["message.type"]
	params := event.Events["message.parameters"]

	items := len(queryIds)

	for i := 0; i < items; i++ {
		query_params := parseParams(params, queryIds[i])
		queries = append(queries, Query{source[0], connections[i], chains[i], queryIds[i], types[i], query_params})
	}

	for _, q := range queries {
		go doRequest(q)
	}
}

func parseParams(params []string, query_id string) map[string]string {
	out := map[string]string{}
	for _, p := range params {
		if strings.HasPrefix(p, query_id) {
			parts := strings.SplitN(p, ":", 3)
			out[parts[1]] = parts[2]
		}
	}
	return out
}

func doRequest(query Query) {
	client := clients.GetForChainId(query.ChainId)
	if client == nil {
		fmt.Println("No chain")
		return
	}
	fmt.Println(query.Type)
	var data []byte

	switch query.Type {
	case "cosmos.bank.v1beta1.Query/AllBalances":
		balances, _ := client.QueryAllBalances(query.Params["address"])
		data = client.Codec.Marshaler.MustMarshalJSON(balances)

	case "cosmos.tx.v1beta1.Query/GetTxEvents":
		txs, err := client.QueryTxs(1, 100, convertParamsToEvents(query.Params))
		if err != nil {
			panic(err)
		}

		data, err = json.Marshal(txs)
		if err != nil {
			panic(err)
		}
	case "cosmos.staking.v1beta1.Query/DelegatorDelegations":
		delegations, _ := staking.QueryDelegations(client, query.Params["address"], lensclient.DefaultPageRequest())
		data = client.Codec.Marshaler.MustMarshalJSON(delegations)

	case "cosmos.staking.v1beta1.Query/Validators":
		validators, err := staking.QueryValidators(client, query.Params["status"], lensclient.DefaultPageRequest())
		if err != nil {
			panic(err)
		}
		data = client.Codec.Marshaler.MustMarshalJSON(validators)

	default:
		fmt.Println("Unexpected query type: ", query.Type)
	}

	// submit tx to queue
	submitClient := clients.GetForChainId(query.SourceChainId)
	from, _ := submitClient.GetKeyAddress()
	msg := &qstypes.MsgSubmitQueryResponse{query.ChainId, query.QueryId, data, 0, submitClient.MustEncodeAccAddr(from)}
	sendQueue[query.SourceChainId] <- msg
}

func convertParamsToEvents(params map[string]string) []string {
	out := []string{}
	for k, v := range params {
		out = append(out, fmt.Sprintf("%s='%s'", k, v))
	}
	return out
}

func FlushSendQueue(chainId string) {
	time.Sleep(WaitInterval)
	toSend := []sdk.Msg{}
	ch := sendQueue[chainId]

	for {
		if len(toSend) > 10 {
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
		resp, err := client.SendMsgs(context.Background(), unique(toSend))
		fmt.Println(resp)
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
	list := []sdk.Msg{}
	for _, entry := range msgSlice {
		msg := entry.(*qstypes.MsgSubmitQueryResponse)
		if _, value := keys[msg.QueryId]; !value {
			keys[msg.QueryId] = true
			list = append(list, entry)
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
