package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"

	mgmt "github.com/pion/ion/apps/room-mgmt/server"

	log "github.com/pion/ion-log"
)

// run as distributed node
func main() {
	var confFile string
	flag.StringVar(&confFile, "c", "app-room-mgmt.toml", "config file")
	flag.Parse()

	if confFile == "" {
		flag.PrintDefaults()
		return
	}

	conf := mgmt.Config{}
	err := conf.Load(confFile)
	if err != nil {
		log.Errorf("config load error: %s", err)
		return
	}

	log.Init(conf.Log.Level)
	log.Infof("--- Starting Room-Mgmt ---")

	node := mgmt.New()
	if err := node.Start(conf); err != nil {
		log.Errorf("room-mgmt init start: %s", err)
		os.Exit(1)
	}

	// Press Ctrl+C to exit the process
	quitCh := make(chan os.Signal, 1)
	signal.Notify(quitCh, os.Interrupt, syscall.SIGTERM)
	<-quitCh
}
