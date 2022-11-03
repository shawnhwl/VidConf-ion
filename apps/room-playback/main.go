package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"

	log "github.com/pion/ion-log"
	"github.com/pion/ion/apps/room-playback/playback"
)

// run as distributed node
func main() {
	var confFile string
	flag.StringVar(&confFile, "c", "app-room-playback.toml", "config file")
	flag.Parse()

	if confFile == "" {
		flag.PrintDefaults()
		return
	}

	conf := playback.Config{}
	err := conf.Load(confFile)
	if err != nil {
		log.Errorf("config load error: %v", err)
		return
	}

	log.Init(conf.Log.Level)
	log.Infof("--- Starting Room-Playback ---")

	quitCh := make(chan os.Signal, 1)
	node := playback.New()
	if err := node.Start(conf, quitCh); err != nil {
		log.Errorf("room-playback init start: %v", err)
		os.Exit(-1)
	}
	defer node.Close()

	// Press Ctrl+C to exit the process
	signal.Notify(quitCh, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-quitCh
	// s.quitCh <- syscall.SIGTERM
}
