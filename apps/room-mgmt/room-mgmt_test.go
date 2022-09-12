package main

import (
	"flag"
	"fmt"
	"testing"

	"github.com/pion/ion/apps/room-mgmt/server"
)

func TestRoomSignal(t *testing.T) {
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
		t.Fatalf("config load error: %v", err)
		return
	}

	roomMgmtService := *server.NewRoomMgmtService(conf)

	roomname := "testroom"
	reason := "testreason"
	msg := "testmsg"
	displayname := "testuser"

	fmt.Printf("\n====================End missing Room=================\n")
	roomMgmtService.endRoom("missing", reason)
	fmt.Printf("\n====================Prompt message in missing room=================\n")
	promptRoom("missing", msg)
	fmt.Printf("\n====================Kick missing User in missing room=================\n")
	kickUser("missing", "missing")
	fmt.Printf("\n====================Peer Info=================\n")
	peerinfo := roomService.GetPeers(roomname)
	fmt.Printf("\nPeers:%v", peerinfo)
	fmt.Printf("\n====================CreateRoom=================\n")
	createRoom(roomname)
	fmt.Printf("\n====================Peer Info=================\n")
	peerinfo = roomService.GetPeers(roomname)
	fmt.Printf("\nPeers:%v", peerinfo)
	fmt.Printf("\n====================Add Peers=================\n")
	err = roomService.AddPeer(sdk.PeerInfo{Sid: roomname, Uid: "123456", DisplayName: "abc"})
	if err != nil {
		fmt.Printf("\nerror: %v", err)
	}
	err = roomService.AddPeer(sdk.PeerInfo{Sid: roomname, Uid: "234567", DisplayName: "def"})
	if err != nil {
		fmt.Printf("\nerror: %v", err)
	}
	err = roomService.AddPeer(sdk.PeerInfo{Sid: roomname, Uid: "345678", DisplayName: displayname})
	if err != nil {
		fmt.Printf("\nerror: %v", err)
	}
	fmt.Printf("\n====================Peer Info=================\n")
	peerinfo = roomService.GetPeers(roomname)
	fmt.Printf("\nPeers:%v", peerinfo)
	fmt.Printf("\n====================Kick User=================\n")
	kickUser(roomname, displayname)
	fmt.Printf("\n====================Peer Info=================\n")
	peerinfo = roomService.GetPeers(roomname)
	fmt.Printf("\nPeers:%v", peerinfo)
	fmt.Printf("\n====================Kick User Again=================\n")
	kickUser(roomname, displayname)
	fmt.Printf("\n====================Peer Info=================\n")
	peerinfo = roomService.GetPeers(roomname)
	fmt.Printf("\nPeers:%v", peerinfo)
	fmt.Printf("\n====================Prompt message=================\n")
	promptRoom(roomname, msg)
	fmt.Printf("\n====================CreateRoom Again=================\n")
	createRoom(roomname)
	fmt.Printf("\n====================Peer Info=================\n")
	peerinfo = roomService.GetPeers(roomname)
	fmt.Printf("\nPeers:%v", peerinfo)
	fmt.Printf("\n====================End Room=================\n")
	endRoom(roomname, reason)
	fmt.Printf("\n====================CreateRoom Again Finally=================\n")
	createRoom(roomname)
	peerinfo = roomService.GetPeers(roomname)
	fmt.Printf("\nPeers:%v", peerinfo)
	fmt.Printf("\n====================End Room Finally=================\n")
	endRoom(roomname, reason)
}
