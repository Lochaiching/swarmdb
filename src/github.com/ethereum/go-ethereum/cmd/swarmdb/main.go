package main

import (
	"flag"
	"fmt"
	"github.com/ethereum/go-ethereum/log"
	swarmdb "github.com/ethereum/go-ethereum/swarmdb"
	"github.com/ethereum/go-ethereum/swarmdb/server"
	"os"
)

func main() {
	configFileLocation := flag.String("config", swarmdb.SWARMDBCONF_FILE, "Full path location to SWARMDB configuration file.")
	//TODO: store this somewhere accessible to be used later
	logLevelFlag := flag.Int("loglevel", 3, "Log Level Verbosity 1-6 (4 for debug)")
	version := flag.Bool("v", false, "Prints current SWARMDB version")
	flag.Parse()

	if *version {
		log.Debug(fmt.Sprintf("Working on version %s of SWARMDB Sever\n", swarmdb.SWARMDBVersion))
		fmt.Printf("Working on version %s of SWARMDB Sever\n", swarmdb.SWARMDBVersion)
		os.Exit(0)
	}
	if _, err := os.Stat(*configFileLocation); os.IsNotExist(err) {
		log.Debug("Default config file missing.  Building ..")
		_, err := swarmdb.NewKeyManagerWithoutConfig(*configFileLocation, swarmdb.SWARMDBCONF_DEFAULT_PASSPHRASE)
		if err != nil {
			//TODO
		}
	}

	config, err := swarmdb.LoadSWARMDBConfig(*configFileLocation)
	if err != nil {
		log.Debug("The config file location provided [%s] is invalid.  Exiting ...", *configFileLocation)
		os.Exit(1)
	}

	log.Root().SetHandler(log.LvlFilterHandler(log.Lvl(*logLevelFlag), log.StreamHandler(os.Stderr, log.TerminalFormat(false))))
	log.Debug(fmt.Sprintf("Starting SWARMDB (Version: %s) using [%s] and loglevel [%d]", swarmdb.SWARMDBVersion, *configFileLocation, *logLevelFlag))

	swdb, err := swarmdb.NewSwarmDB(&config, nil)
	if err != nil {
		panic(fmt.Sprintf("Cannot start: %s", err.Error()))
	}
	log.Debug("Trying to start HttpServer")
	go server.StartHttpServer(swdb, &config)

	log.Debug("Trying to start TCPIP server...\n")
	server.StartTcpipServer(swdb, &config)
}
