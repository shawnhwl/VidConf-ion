package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/pion/ion/apps/room-mgmt/server"

	log "github.com/pion/ion-log"
)

// run as distributed node
func main() {
	var confFile, logLevel string
	flag.StringVar(&confFile, "c", "app-room-mgmt.toml", "config file")
	flag.StringVar(&logLevel, "l", "info", "log level")
	flag.Parse()

	flag.Parse()

	if confFile == "" {
		flag.PrintDefaults()
		return
	}

	conf := server.Config{}
	err := conf.Load(confFile)
	if err != nil {
		log.Errorf("config load error: %v", err)
		return
	}

	log.Init(conf.Log.Level)
	log.Infof("--- Starting Room-Mgmt HTTP-API Server ---")

	node := server.New()
	if err := node.Start(conf); err != nil {
		log.Errorf("room-mgmt init start: %v", err)
		os.Exit(-1)
	}
	defer node.Close()

	// Press Ctrl+C to exit the process
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	<-ch
}
