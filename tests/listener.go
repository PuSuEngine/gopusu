package main

import (
	"github.com/PuSuEngine/gopusu"
	"fmt"
	"log"
	"time"
	"os"
)

var received = 0

func main() {
	log.Printf("Connecting to 127.0.0.1:55000")
    pc, _ := gopusu.NewClient("127.0.0.1", 55000)
	pc.OnDisconnect(onDisconnect)
	pc.OnError(onError)
    defer pc.Close()
	log.Printf("Authorizing with 'foo'")
    pc.Authorize("foo")
	log.Printf("Subscribing to channel.1")
    pc.Subscribe("channel.1", listener)

	log.Printf("Waiting for messages")

	for i := 0; i < 600; i++ {
		time.Sleep(time.Second * 10)
		log.Printf("Got %d messages", received)
	}
}

func onDisconnect() {
	log.Println("Disconnected from server. Quitting.")
	os.Exit(0)
}

func onError(errType string) {
	log.Printf("Got error %s\n", errType)
}

func listener(msg *gopusu.Publish) {
	received++

	if received % 500 == 0 {
		fmt.Print(".")
	}
}
