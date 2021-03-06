package main

import (
	"fmt"
	"log"
	"time"
	"github.com/PuSuEngine/gopusu"
	"os"
)

func main() {
	delay := 10 * time.Millisecond
	sleepInterval := 25
	for {
		log.Printf("Starting test with %s delay every %d messages\n", delay, sleepInterval)
		time.Sleep(time.Second)
		pc, err := gopusu.NewClient("127.0.0.1", 55000)
		pc.OnDisconnect(onDisconnect)
		pc.OnError(onError)

		if err != nil {
			log.Println(err)
			log.Fatalf("Failed to create PuSuClient\n")
		}

		defer pc.Close()

		err = pc.Authorize("foo")

		if err != nil {
			log.Println(err)
			log.Fatalf("Failed to authorize\n")
		}

		log.Println("Sending messages")

		messages := 10000

		start := time.Now()
		for i := 0; i < messages; i++ {
			if i%500 == 0 {
				fmt.Print(".")
			}
			pc.Publish("channel.1", fmt.Sprintf("message %d", i))
			if i%sleepInterval == 0 {
				time.Sleep(delay)
			}
		}
		fmt.Print("\n")

		since := time.Since(start)
		duration := since / time.Duration(messages)
		rate := int64(time.Second / duration)

		log.Printf("Sent %d messages in %s", messages, since)
		log.Printf("%s/message", duration)
		log.Printf("%d messages/sec", rate)

		if delay > time.Millisecond {
			delay -= time.Millisecond
		} else {
			sleepInterval += 5
		}
	}
}

func onDisconnect() {
	log.Println("Disconnected from server. Quitting.")
	os.Exit(0)
}

func onError(errType string) {
	log.Printf("Got error %s\n", errType)
}
