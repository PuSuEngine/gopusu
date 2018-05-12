// The Golang client for PuSu Engine. PuSu Engine is a (relatively) fast and
// scalable Pub-Sub message delivery system.
//
// The Golang client is a simple (mostly) synchronous client that does little
// magic internally. For operations that the server acknowledges (Connect,
// Authorize, Subscribe) it waits for the appropriate event coming back from
// the server before continuing, to ensure you don't do stupid things and get
// unexpected results.
//
// Example usage
//
//  package main
//
//  import (
//      "fmt"
//      "github.com/PuSuEngine/gopusu"
//  )
//
//  func main() {
//      pc, _ := gopusu.NewPuSuClient("127.0.0.1", 55000)
//      defer pc.Close()
//      pc.Authorize("foo")
//      pc.Subscribe("channel.1", listener)
//      pc.Publish("channel.2", "message")
//  }
//
//  func listener(msg *gopusu.Publish) {
//      fmt.Printf("Got message %s on channel %s\n", msg.Content, msg.Channel)
//  }
package gopusu

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/PuSuEngine/pusud/messages"
	"github.com/gorilla/websocket"
	"net/url"
	"sync"
	"time"
)

const DEBUG = false
const DEFAULT_TIMEOUT = time.Second * 5

// Timeout exceeded when waiting to acknowledge Authorize/Subscribe request
var ErrTimeoutExceeded = errors.New("Timeout exceeded when waiting for server to acknowledge message")

// Callback to call with the published messages in a
// channel we're subscribed to.
type SubscribeCallback func(*Publish)
type subscribers map[string]SubscribeCallback

// The PuSu client. Create one of these with NewClient(),
// and call the Authorize(), Subscribe() and Publish()
// methods to communicate with the PuSu network.
type Client struct {
	connection   *websocket.Conn
	server       string
	subscribers  subscribers
	Timeout      time.Duration
	waiting      bool
	waitingChs   []chan waitEvent
	waiterMutex  sync.Mutex
	connected    bool
	onDisconnect func()
	onError      func(string)
}

// Published message from the PuSu network. You get these
// to your callback if you subscribe to a channel.
type Publish struct {
	Type    string `json:"type"`
	Channel string `json:"channel"`
	Content string `json:"content"`
}

type waitEvent struct {
	Type    string
	Channel string
}

// Claim you have authorization to access some things.
// The server will determine what those things could be
// based on the configured authenticator and the data you
// give. Expect to get disconnected if this is invalid.
func (pc *Client) Authorize(authorization string) error {
	msg := messages.Authorize{
		Type:          messages.TYPE_AUTHORIZE,
		Authorization: authorization,
	}
	err := pc.sendMessage(&msg)

	if err != nil {
		return err
	}

	err = pc.wait(messages.TYPE_AUTHORIZATION_OK, "")

	if err != nil {
		return err
	}

	return nil
}

// Ask to subscribe for messages on the given channel. You
// MUST use the Authorize() method before this, even if
// server is configured to use the "None" authenticator.
// Expect to get disconnected if you lack the permissions.
func (pc *Client) Subscribe(channel string, callback SubscribeCallback) error {
	pc.subscribers[channel] = callback

	msg := messages.Subscribe{
		Type:    messages.TYPE_SUBSCRIBE,
		Channel: channel,
	}
	err := pc.sendMessage(&msg)

	if err != nil {
		return err
	}

	err = pc.wait(messages.TYPE_SUBSCRIBE_OK, channel)

	if err != nil {
		return err
	}

	return nil
}

// Publish a message on the given channel
func (pc *Client) Publish(channel string, content string) error {
	// TODO: Support interface{} for content
	msg := messages.Publish{
		Type:    messages.TYPE_PUBLISH,
		Channel: channel,
		Content: content,
	}
	return pc.sendMessage(&msg)
}

// Disconnect from the server
func (pc *Client) Close() {
	pc.connected = false
	if pc.connection != nil {
		pc.connection.Close()
		pc.connection = nil
	}
}

func (pc *Client) OnDisconnect(listener func()) {
	pc.onDisconnect = listener
}

func (pc *Client) OnError(listener func(string)) {
	pc.onError = listener
}

func (pc *Client) sendMessage(message messages.Message) (err error) {
	if pc.connection == nil {
		return
	}

	data := message.ToJson()

	if DEBUG {
		fmt.Printf("-> %s\n", data)
	}

	err = pc.connection.WriteMessage(websocket.TextMessage, data)

	return
}

func (pc *Client) disconnected() {
	pc.connected = false
	if DEBUG {
		fmt.Printf("Disconnected from server.\n")
	}
	if pc.onDisconnect != nil {
		pc.onDisconnect()
	}
}

func (pc *Client) error(errType string) {
	if DEBUG {
		fmt.Printf("Got error from server: %s.\n", errType)
	}

	if pc.onError != nil {
		pc.onError(errType)
	}
}

func (pc *Client) receive(data []byte) {
	if DEBUG {
		fmt.Printf("<- %s\n", data)
	}

	m := messages.GenericMessage{}
	json.Unmarshal(data, &m)

	if len(pc.waitingChs) > 0 {
		e := waitEvent{}
		json.Unmarshal(data, &e)

		pc.waiterMutex.Lock()
		for _, ch := range pc.waitingChs {
			ch <- e
		}
		pc.waiterMutex.Unlock()
	}

	if m.Type == messages.TYPE_PUBLISH {
		p := Publish{}
		json.Unmarshal(data, &p)

		callback, ok := pc.subscribers[p.Channel]
		if ok {
			callback(&p)
		}
	}

	if m.Type == messages.TYPE_AUTHORIZATION_FAILED || m.Type == messages.TYPE_PERMISSION_DENIED || m.Type == messages.TYPE_UNKNOWN_MESSAGE_RECEIVED {
		pc.error(m.Type)
	}
}

func (pc *Client) addWaiter(ch chan waitEvent) {
	pc.waiterMutex.Lock()
	defer pc.waiterMutex.Unlock()
	pc.waitingChs = append(pc.waitingChs, ch)
}

func (pc *Client) removeWaiter(ch chan waitEvent) {
	pc.waiterMutex.Lock()
	defer pc.waiterMutex.Unlock()

	chs := []chan waitEvent{}
	for _, c := range pc.waitingChs {
		if c != ch {
			chs = append(chs, c)
		}
	}
	pc.waitingChs = chs
}

func (pc *Client) wait(eventType string, channel string) (err error) {
	ch := make(chan waitEvent)
	pc.addWaiter(ch)
	defer pc.removeWaiter(ch)

	if DEBUG {
		if channel != "" {
			fmt.Printf("Waiting for %s on %s\n", eventType, channel)
		} else {
			fmt.Printf("Waiting for %s\n", eventType)
		}
	}

	select {
	case e := <-ch:
		if e.Type == eventType && e.Channel == channel {
			if DEBUG {
				fmt.Printf("Got %s for %s\n", e.Type, e.Channel)
			}
			return
		}
	case <-time.After(pc.Timeout):
		err = ErrTimeoutExceeded
		return
	}

	return
}

// Connect to the server
func (pc *Client) Connect() (err error) {

	if DEBUG {
		fmt.Printf("Connecting to %s\n", pc.server)
	}

	c, _, err := websocket.DefaultDialer.Dial(pc.server, nil)

	if err != nil {
		return
	}

	pc.connection = c

	go func() {
		for {
			_, message, err := c.ReadMessage()
			if err != nil {
				pc.disconnected()
				return
			}
			pc.receive(message)
		}
	}()

	err = pc.wait(messages.TYPE_HELLO, "")

	if err != nil {
		pc.connected = true
	}

	return
}

// Create a new PuSu client and connect to the given server
func NewClient(host string, port int) (pc *Client, err error) {
	addr := fmt.Sprintf("%s:%d", host, port)
	u := url.URL{Scheme: "ws", Host: addr, Path: "/"}

	pc = &Client{}
	pc.waitingChs = []chan waitEvent{}
	pc.subscribers = subscribers{}
	pc.Timeout = DEFAULT_TIMEOUT
	pc.server = u.String()

	err = pc.Connect()

	return
}
