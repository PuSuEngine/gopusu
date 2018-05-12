package main

import (
	"github.com/PuSuEngine/gopusu"
	"log"
	"time"
)

func main() {
	for {
		time.Sleep(time.Second)
		log.Printf("Connecting to 127.0.0.1:55000")
		pc, err := gopusu.NewClient("127.0.0.1", 55000)
		pc.OnDisconnect(onDisconnect)
		pc.OnError(onError)

		if err != nil {
			log.Fatalf("error: %s", err)
		}

		defer pc.Close()

		log.Printf("Authorizing with 'foo'")
		err = pc.Authorize("foo")
		if err != nil {
			log.Printf("error: %s\n", err)
			continue
		}

		log.Printf("Subscribing to channel.1")
		err = pc.Subscribe("channel.1", listener)
		go func() {
			err = pc.Subscribe("channel.2", func(msg *gopusu.Publish) {
				// Intentionally empty
			})
		}()
		if err != nil {
			log.Printf("error: %s\n", err)
			continue
		}

		log.Printf("Sending message to channel.1")
		err = pc.Publish("channel.1", "message")
		if err != nil {
			log.Printf("error: %s\n", err)
			continue
		}

		log.Printf("Waiting for messages")
		time.Sleep(time.Second)
	}
}

func onDisconnect() {
	log.Println("Disconnected from server.")
}

func onError(errType string) {
	log.Printf("Got error %s\n", errType)
}

func listener(msg *gopusu.Publish) {
	log.Printf("Got message %s on channel %s", msg.Content, msg.Channel)
}
