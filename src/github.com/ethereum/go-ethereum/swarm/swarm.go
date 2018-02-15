// Copyright 2016 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package swarm

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	//"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/contracts/chequebook"
	"github.com/ethereum/go-ethereum/contracts/ens"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/node"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/discover"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/ethereum/go-ethereum/swarm/api"
	httpapi "github.com/ethereum/go-ethereum/swarm/api/http"
	"github.com/ethereum/go-ethereum/swarm/fuse"
	"github.com/ethereum/go-ethereum/swarm/network"
	"github.com/ethereum/go-ethereum/swarm/storage"
	"github.com/ethereum/go-ethereum/swarmdb"
	swarmdbserver "github.com/ethereum/go-ethereum/swarmdb/server"
	//"github.com/ethereum/go-ethereum/swarmdb/server/tcpip"

	//"github.com/syndtr/goleveldb/leveldb"
	//"reflect"
)

// the swarm stack
type Swarm struct {
	config      *api.Config            // swarm configuration
	api         *api.Api               // high level api layer (fs/manifest)
	dns         api.Resolver           // DNS registrar
	dbAccess    *network.DbAccess      // access to local chunk db iterator and storage counter
	storage     storage.ChunkStore     // internal access to storage, common interface to cloud storage backends
	dpa         *storage.DPA           // distributed preimage archive, the local API to the storage with document level storage/retrieval support
	depo        network.StorageHandler // remote request handler, interface between bzz protocol and the storage
	cloud       storage.CloudStore     // procurement, cloud storage backend (can multi-cloud)
	hive        *network.Hive          // the logistic manager
	backend     chequebook.Backend     // simple blockchain Backend
	privateKey  *ecdsa.PrivateKey
	corsString  string
	swapEnabled bool
	lstore      *storage.LocalStore // local store, needs to store for releasing resources after node stopped
	sfs         *fuse.SwarmFS       // need this to cleanup all the active mounts on node exit
	ldb         *storage.LDBDatabase
	swarmdb     *swarmdb.SwarmDB
	sdbstorage  storage.ChunkStore
}

type SwarmAPI struct {
	Api     *api.Api
	Backend chequebook.Backend
	PrvKey  *ecdsa.PrivateKey
}

func (self *Swarm) API() *SwarmAPI {
	return &SwarmAPI{
		Api:     self.api,
		Backend: self.backend,
		PrvKey:  self.privateKey,
	}
}

// creates a new swarm service instance
// implements node.Service
func NewSwarm(ctx *node.ServiceContext, backend chequebook.Backend, ensClient *ethclient.Client, config *api.Config, swapEnabled, syncEnabled bool, cors string) (self *Swarm, err error) {
	if bytes.Equal(common.FromHex(config.PublicKey), storage.ZeroKey) {
		return nil, fmt.Errorf("empty public key")
	}
	if bytes.Equal(common.FromHex(config.BzzKey), storage.ZeroKey) {
		return nil, fmt.Errorf("empty bzz key")
	}

	self = &Swarm{
		config:      config,
		swapEnabled: swapEnabled,
		backend:     backend,
		privateKey:  config.Swap.PrivateKey(),
		corsString:  cors,
	}
	log.Debug(fmt.Sprintf("Setting up Swarm service components"))

	hash := storage.MakeHashFunc(config.ChunkerParams.Hash)
	self.lstore, err = storage.NewLocalStore(hash, config.StoreParams)
	if err != nil {
		return
	}

	// setup local store
	log.Debug(fmt.Sprintf("Set up local storage"))


	// set up the kademlia hive
	self.hive = network.NewHive(
		common.HexToHash(self.config.BzzKey), // key to hive (kademlia base address)
		config.HiveParams,                    // configuration parameters
		swapEnabled,                          // SWAP enabled
		syncEnabled,                          // syncronisation enabled
	)
	log.Debug(fmt.Sprintf("Set up swarm network with Kademlia hive"))

	// setup cloud storage backend
	cloud := network.NewForwarder(self.hive)
	//self.cloud = cloud
	log.Debug(fmt.Sprintf("-> set swarm forwarder as cloud storage backend"))
	// setup cloud storage internal access layer

	self.storage = storage.NewNetStore(hash, self.lstore, cloud, config.StoreParams)
	self.sdbstorage = storage.NewSdbStore(self.lstore, cloud)
	log.Debug(fmt.Sprintf("-> swarm net store shared access layer to Swarm Chunk Store"))

	// setup swarmdb
/*
	sdbconfigFileLocation := flag.String("config", swarmdb.SWARMDBCONF_FILE, "Full path location to SWARMDB configuration file.")
	logLevelFlag := flag.Int("loglevel", 3, "Log Level Verbosity 1-6 (4 for debug)")
	flag.Parse()
*/
	if _, err := os.Stat(swarmdb.SWARMDBCONF_FILE); os.IsNotExist(err) {
		log.Debug("Default config file missing.  Building ..")
		_, err := swarmdb.NewKeyManagerWithoutConfig(swarmdb.SWARMDBCONF_FILE, swarmdb.SWARMDBCONF_DEFAULT_PASSPHRASE)
		if err != nil {
			//TODO
		}
	}	
	sdbconfig, err := swarmdb.LoadSWARMDBConfig(swarmdb.SWARMDBCONF_FILE)
	log.Debug(fmt.Sprintf("Starting SWARMDB (Version: %s) using [%s]", swarmdb.SWARMDBVersion, swarmdb.SWARMDBCONF_FILE))
	log.Debug(fmt.Sprintf("swarmdb config %v", sdbconfig))
	self.swarmdb, err = swarmdb.NewSwarmDB(&sdbconfig, self.sdbstorage)
	log.Debug(fmt.Sprintf("start SwarmDB err = %v", err))
	//self.swarmdb = swarmdb.NewSwarmDB(self.sdbstorage)
	// set up Depo (storage handler = cloud storage access layer for incoming remote requests)
	self.dbAccess = network.NewDbAccess(self.lstore, self.swarmdb)
	log.Debug(fmt.Sprintf("Set up local db access (iterator/counter)"))
	self.depo = network.NewDepo(hash, self.lstore, self.storage, self.sdbstorage, self.swarmdb)
	self.ldb, _ = storage.NewLDBDatabase(filepath.Join(self.config.Path, "ldb"))
	log.Debug(fmt.Sprintf("-> REmote Access to CHunks"))

	// set up DPA, the cloud storage local access layer
	dpaChunkStore := storage.NewDpaChunkStore(self.lstore, self.storage)
	log.Debug(fmt.Sprintf("-> Local Access to Swarm"))
	// Swarm Hash Merklised Chunking for Arbitrary-length Document/File storage
	self.dpa = storage.NewDPA(dpaChunkStore, self.config.ChunkerParams)
	log.Debug(fmt.Sprintf("-> Content Store API"))

	// set up high level api
	transactOpts := bind.NewKeyedTransactor(self.privateKey)

	log.Debug(fmt.Sprintf("ENS: %v %v %v", transactOpts, config.EnsRoot, ensClient))
	if ensClient == nil {
		log.Warn("No ENS, please specify non-empty --ens-api to use domain name resolution")
	} else {
		self.dns, err = ens.NewENS(transactOpts, config.EnsRoot, ensClient)
		if err != nil {
			return nil, err
		}
	}
	self.api = api.NewApiTest(self.dpa, self.dns, self.ldb)
	// Manifests for Smart Hosting
	log.Debug(fmt.Sprintf("-> Web3 virtual server API"))
	self.sfs = fuse.NewSwarmFS(self.api)
	log.Debug("-> Initializing Fuse file system")

	return self, nil
}

/*
Start is called when the stack is started
* starts the network kademlia hive peer management
* (starts netStore level 0 api)
* starts DPA level 1 api (chunking -> store/retrieve requests)
* (starts level 2 api)
* starts http proxy server
* registers url scheme handlers for bzz, etc
* TODO: start subservices like sword, swear, swarmdns
*/
// implements the node.Service interface
func (self *Swarm) Start(srv *p2p.Server) error {
	connectPeer := func(url string) error {
		node, err := discover.ParseNode(url)
		if err != nil {
			return fmt.Errorf("invalid node URL: %v", err)
		}
		srv.AddPeer(node)
		return nil
	}
	// set chequebook
	if self.swapEnabled {
		ctx := context.Background() // The initial setup has no deadline.
		err := self.SetChequebook(ctx)
		if err != nil {
			return fmt.Errorf("Unable to set chequebook for SWAP: %v", err)
		}
		log.Debug(fmt.Sprintf("-> cheque book for SWAP: %v", self.config.Swap.Chequebook()))
	} else {
		log.Debug(fmt.Sprintf("SWAP disabled: no cheque book set"))
	}

	log.Warn(fmt.Sprintf("Starting Swarm service"))
	self.hive.Start(
		discover.PubkeyID(&srv.PrivateKey.PublicKey),
		func() string { return srv.ListenAddr },
		connectPeer,
	)
	log.Info(fmt.Sprintf("Swarm network started on bzz address: %v", self.hive.Addr()))

	self.dpa.Start()
	log.Debug(fmt.Sprintf("Swarm DPA started"))

	// start swarm http proxy server
	if self.config.Port != "" {
		addr := net.JoinHostPort(self.config.ListenAddr, self.config.Port)
		go httpapi.StartHttpServer(self.api, &httpapi.ServerConfig{
			Addr:       addr,
			CorsString: self.corsString,
		})
		log.Info(fmt.Sprintf("Swarm http proxy started on %v", addr))

		if self.corsString != "" {
			log.Debug(fmt.Sprintf("Swarm http proxy started with corsdomain: %v", self.corsString))
		}
	}
/// swarmdb tcpip, http
	go swarmdbserver.StartHttpServer(self.swarmdb, self.swarmdb.Config)
	go swarmdbserver.StartTcpipServer(self.swarmdb, self.swarmdb.Config)

	return nil
}

// implements the node.Service interface
// stops all component services.
func (self *Swarm) Stop() error {
	self.dpa.Stop()
	self.hive.Stop()
	if ch := self.config.Swap.Chequebook(); ch != nil {
		ch.Stop()
		ch.Save()
	}

	if self.lstore != nil {
		self.lstore.DbStore.Close()
	}
	self.sfs.Stop()
	return self.config.Save()
}

// implements the node.Service interface
func (self *Swarm) Protocols() []p2p.Protocol {
	proto, err := network.Bzz(self.depo, self.backend, self.hive, self.dbAccess, self.config.Swap, self.config.SyncParams, self.config.NetworkId)
	if err != nil {
		return nil
	}
	return []p2p.Protocol{proto}
}

// implements node.Service
// Apis returns the RPC Api descriptors the Swarm implementation offers
func (self *Swarm) APIs() []rpc.API {
	return []rpc.API{
		// public APIs
		{
			Namespace: "bzz",
			Version:   "0.1",
			Service:   &Info{self.config, chequebook.ContractParams},
			Public:    true,
		},
		// admin APIs
		{
			Namespace: "bzz",
			Version:   "0.1",
			Service:   api.NewControl(self.api, self.hive),
			Public:    false,
		},
		{
			Namespace: "chequebook",
			Version:   chequebook.Version,
			Service:   chequebook.NewApi(self.config.Swap.Chequebook),
			Public:    false,
		},
		{
			Namespace: "swarmfs",
			Version:   fuse.Swarmfs_Version,
			Service:   self.sfs,
			Public:    false,
		},
		// storage APIs
		// DEPRECATED: Use the HTTP API instead
		{
			Namespace: "bzz",
			Version:   "0.1",
			Service:   api.NewStorage(self.api),
			Public:    true,
		},
		{
			Namespace: "bzz",
			Version:   "0.1",
			Service:   api.NewFileSystem(self.api),
			Public:    false,
		},
		// {Namespace, Version, api.NewAdmin(self), false},
	}
}

func (self *Swarm) Api() *api.Api {
	return self.api
}

// SetChequebook ensures that the local checquebook is set up on chain.
func (self *Swarm) SetChequebook(ctx context.Context) error {
	err := self.config.Swap.SetChequebook(ctx, self.backend, self.config.Path)
	if err != nil {
		return err
	}
	log.Info(fmt.Sprintf("new chequebook set (%v): saving config file, resetting all connections in the hive", self.config.Swap.Contract.Hex()))
	self.config.Save()
	self.hive.DropAll()
	return nil
}

// Local swarm without netStore
func NewLocalSwarm(datadir, port string) (self *Swarm, err error) {

	prvKey, err := crypto.GenerateKey()
	if err != nil {
		return
	}

	config, err := api.NewConfig(datadir, common.Address{}, prvKey, network.NetworkId)
	if err != nil {
		return
	}
	config.Port = port

	dpa, err := storage.NewLocalDPA(datadir)
	if err != nil {
		return
	}

	self = &Swarm{
		api:    api.NewApi(dpa, nil),
		config: config,
	}

	return
}

// serialisable info about swarm
type Info struct {
	*api.Config
	*chequebook.Params
}

func (self *Info) Info() *Info {
	return self
}
