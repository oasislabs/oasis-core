package oasis

import (
	"fmt"

	"github.com/pkg/errors"

	registry "github.com/oasislabs/oasis-core/go/registry/api"
	storageClient "github.com/oasislabs/oasis-core/go/storage/client"
)

// Client is an Oasis client node.
type Client struct {
	Node

	consensusPort uint16
}

func (client *Client) startNode() error {
	args := newArgBuilder().
		debugAllowTestKeys().
		tendermintCoreListenAddress(client.consensusPort).
		roothashTendermintIndexBlocks().
		storageBackend(storageClient.BackendName).
		appendNetwork(client.net)
	for _, v := range client.net.runtimes {
		if v.kind != registry.KindCompute {
			continue
		}
		args = args.clientIndexRuntimes(v.id)
	}

	var err error
	if client.cmd, client.exitCh, err = client.net.startOasisNode(client.dir, nil, args, "client", false, false); err != nil {
		return errors.Wrap(err, "oasis/client: failed to launch node")
	}

	return nil
}

// NewClient provisions a new client node and adds it to the network.
func (net *Network) NewClient() (*Client, error) {
	clientName := fmt.Sprintf("client-%d", len(net.clients))

	clientDir, err := net.baseDir.NewSubDir(clientName)
	if err != nil {
		net.logger.Error("failed to create client subdir",
			"err", err,
			"client_name", clientName,
		)
		return nil, errors.Wrap(err, "oasis/client: failed to create client subdir")
	}

	client := &Client{
		Node: Node{
			net: net,
			dir: clientDir,
		},
		consensusPort: net.nextNodePort,
	}
	client.doStartNode = client.startNode

	net.clients = append(net.clients, client)
	net.nextNodePort++

	return client, nil
}
