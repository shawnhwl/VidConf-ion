package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"

	log "github.com/pion/ion-log"
	"github.com/pion/ion/apps/room-recorder/recorder"
)

// run as distributed node
func main() {
	var confFile string
	flag.StringVar(&confFile, "c", "app-room-recorder.toml", "config file")

	flag.Parse()

	if confFile == "" {
		flag.PrintDefaults()
		return
	}

	conf := recorder.Config{}
	err := conf.Load(confFile)
	if err != nil {
		log.Errorf("config load error: %v", err)
		return
	}

	log.Init(conf.Log.Level)
	log.Infof("--- Starting Room-Recorder ---")

	quitCh := make(chan os.Signal, 1)
	go recorder.NewRoomRecorder(conf, quitCh)

	// Press Ctrl+C to exit the process
	signal.Notify(quitCh, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-quitCh
}
