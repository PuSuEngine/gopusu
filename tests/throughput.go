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
	for {
		log.Printf("Starting test with %dms delay\n", delay / time.Millisecond)
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
			if i%25 == 0 {
				time.Sleep(delay)
			}
		}
		fmt.Print("\n")

		since := time.Since(start)
		msec := since / time.Millisecond
		duration := since / time.Duration(messages)
		rate := int64(time.Second / duration)

		log.Printf("Sent %d messages in %d msec", messages, int64(msec))
		log.Printf("%d usec/message", int64(duration/time.Microsecond))
		log.Printf("%d messages/sec", rate)

		if delay > time.Millisecond {
			delay -= time.Millisecond
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
