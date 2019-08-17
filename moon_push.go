package main

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/websocket"
	"hash/fnv"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

func hash(s *string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(*s))
	return h.Sum32()
}

func makeHandleNotify(state *SharedState) func(w http.ResponseWriter, r *http.Request) {

	return func(w http.ResponseWriter, r *http.Request) {

		if r.Body == nil {
			http.Error(w, "Please send a request body", 400)
			return
		}

		var notifyRequest NotifyRequest

		err := json.NewDecoder(r.Body).Decode(&notifyRequest)
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		//strBytes, _ := json.Marshal(notifyRequest)

		pushMessage := notifyRequest.ToPushMessage(int(time.Now().Unix()))
		eventId := notifyRequest.Notification.EventID
		unread := notifyRequest.Notification.Counts.Unread

		for _, device := range notifyRequest.Notification.Devices {
			token := device.Pushkey
			isClientKnown := state.IsClientKnown(&token)

			targetPushKeyHash := hash(&notifyRequest.Notification.Devices[0].Pushkey)
			log.Printf("notify %d, unread: %d, triggered by event: %t\n", targetPushKeyHash, unread, len(eventId) > 0)

			isEventIdKnown := false
			if isClientKnown {
				log.Printf("set last notify for %d\n", targetPushKeyHash)
				state.SetLastNotify(&token, pushMessage)
				if len(eventId) > 0 {
					isEventIdKnown = state.IsEventIdKnown(&token, &eventId)
				}
			}
			if isClientKnown && isEventIdKnown {
				log.Printf("%s is a duplicate, not sending to %d\n", eventId, targetPushKeyHash)
			} else if isClientKnown {
				log.Printf("send to channel of %d\n", targetPushKeyHash)
				state.SendNotifyToClientChannel(&token, pushMessage)
				state.AddKnownEventId(&token, &eventId)
			} else {
				log.Printf("%d is unknown\n", targetPushKeyHash)
			}
		}

		w.Header().Add("Content-Type", "text/json; charset=utf-8")
		err = json.NewEncoder(w).Encode(NotifyResponse{Rejected: []string{}})
		if err != nil {
			log.Println(err.Error())
		}

	}
}

func makeSendNotifications(state *SharedState) func(w http.ResponseWriter, r *http.Request) {
	var upgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
	}

	return func(w http.ResponseWriter, r *http.Request) {

		tokens, hasTokenParam := r.URL.Query()["token"]
		if !hasTokenParam || len(tokens) == 0 || len(tokens[0]) == 0 {
			http.Error(w, "missing token", 400)
			return
		}

		since := 0
		sinces, hasSinceParam := r.URL.Query()["since"]
		if hasSinceParam && len(sinces) > 0 && len(sinces[0]) > 0 {
			parsed, err := strconv.Atoi(sinces[0])
			if err == nil {
				since = parsed
			}
		}

		w.Header().Set("Cache-Control", "no-cache")

		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Println(err)
			return
		}

		token := tokens[0]
		tokenHash := hash(&token)
		log.Printf("connect since %d for %d\n", since, tokenHash)

		state.NewClient(&token)

		lastNotify := state.GetLastNotify(&token)

		clientChannel, hasChannel := state.GetClientChannel(&token)

		if !hasChannel {
			log.Printf("%d has no connected channel\n", tokenHash)
			return
		}

		if lastNotify != nil && since < lastNotify.Timestamp {
			log.Printf("say hi to %d with last notify", tokenHash)
			state.SendNotifyToClientChannel(&token, lastNotify)
		} else if lastNotify == nil {
			log.Printf("%d no last notify\n", tokenHash)
		} else {
			log.Printf("%d has not missed events\n", tokenHash)
		}

		wsCloseChannel := make(chan bool, 1)
		wsPingChannel := make(chan string, 100)
		wsReadErrorChannel := make(chan bool, 1)

		defaultPingHandler := ws.PingHandler()
		ws.SetPingHandler(func(data string) error {
			wsPingChannel <- data
			return defaultPingHandler(data)
		})

		defaultCloseHandler := ws.CloseHandler()
		ws.SetCloseHandler(func(code int, text string) error {
			wsPingChannel <- text
			return defaultCloseHandler(code, text)
		})

		go func() {
			for {
				_, _, err := ws.ReadMessage()
				if err != nil {
					log.Println(err.Error())
					wsReadErrorChannel <- true
					return
				}
			}
		}()


		errorCount := 0

		for {
			select {
			case <-wsCloseChannel:
				log.Printf("%d ws connection closed\n", tokenHash)
				state.RemoveClientChannel(&token)
				return
			case <-wsReadErrorChannel:
				errorCount ++
				log.Printf("%d ws read error[%d]\n", tokenHash, errorCount)
				if errorCount > 10 {
					state.RemoveClientChannel(&token)
					log.Printf("%d channel removed")
					return
				}
			case <-wsPingChannel:
				errorCount = 0
				//log.Printf("%d sent ping\n", tokenHash)
			case notifyRequest := <-clientChannel:

				log.Printf("send message to %d\n", tokenHash)
				strBytes, _ := json.Marshal(notifyRequest)

				err := ws.WriteMessage(websocket.TextMessage, strBytes)

				if err != nil {
					errorCount ++
					log.Printf("%d write error[%d]: %s\n", tokenHash, errorCount, err.Error())

					if errorCount > 10 {
						state.RemoveClientChannel(&token)
						log.Printf("%d channel removed", tokenHash)
						return
					}
				} else {
					errorCount = 0
				}
			}
		}

	}
}

func makeHandlePollLastNotify(state *SharedState) func(w http.ResponseWriter, r *http.Request) {

	return func(w http.ResponseWriter, r *http.Request) {

		tokens, ok := r.URL.Query()["token"]
		if !ok || len(tokens) == 0 || len(tokens[0]) == 0 {
			http.Error(w, "missing token", 400)
			return
		}
		token := tokens[0]

		sinces, ok := r.URL.Query()["since"]
		if !ok || len(sinces) == 0 || len(sinces[0]) == 0 {
			http.Error(w, "missing since", 400)
			return
		}

		since, parseSinceError := strconv.Atoi(sinces[0])
		if parseSinceError != nil {
			http.Error(w, "since must be int", 400)
			return
		}

		tokenHash := hash(&token)
		log.Printf("poll requset for %d since %d \n", tokenHash, since)

		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Content-Type", "application/json")
		isClientKnown := state.IsClientKnown(&token)

		if !isClientKnown {
			log.Printf("%d is unknown, return 400\n", tokenHash)
			http.Error(w, "invalid token", 400)
			return
		}

		lastNotify := state.GetLastNotify(&token)
		if lastNotify == nil {
			log.Printf("%d never had a message\n", tokenHash)
			return
		}

		if lastNotify.Timestamp <= since {
			log.Printf("%d has no missed message\n", tokenHash)
		}

		strBytes, _ := json.Marshal(lastNotify)
		jsonString := RemoveLineBreaks(string(strBytes))
		_, err := fmt.Fprintf(w, jsonString+"\n")
		if err != nil {
			log.Println(err.Error())
		}
	}
}

func main() {
	log.Println("starting server")

	sharedState := SharedState{
		clients: make(map[string]*ClientState),
	}

	http.HandleFunc("/push_gateway/_matrix/push/v1/notify", makeHandleNotify(&sharedState))
	http.HandleFunc("/push_gateway/notifications/push", makeSendNotifications(&sharedState))
	http.HandleFunc("/push_gateway/notifications/poll", makeHandlePollLastNotify(&sharedState))

	log.Fatal(http.ListenAndServe(":6002", nil))
}

type NotifyRequest struct {
	Notification struct {
		EventID           string `json:"event_id"`
		RoomID            string `json:"room_id"`
		Type              string `json:"type"`
		Sender            string `json:"sender"`
		SenderDisplayName string `json:"sender_display_name"`
		RoomName          string `json:"room_name"`
		RoomAlias         string `json:"room_alias"`
		Prio              string `json:"prio"`
		Content           struct {
			Msgtype string `json:"msgtype"`
			Body    string `json:"body"`
		} `json:"content"`
		Counts struct {
			Unread      int `json:"unread"`
			MissedCalls int `json:"missed_calls"`
		} `json:"counts"`
		Devices []struct {
			AppID     string `json:"app_id"`
			Pushkey   string `json:"pushkey"`
			PushkeyTs int    `json:"pushkey_ts"`
			Data      struct {
			} `json:"data"`
			Tweaks struct {
				Sound string `json:"sound"`
			} `json:"tweaks"`
		} `json:"devices"`
	} `json:"notification"`
}

type NotifyResponse struct {
	Rejected []string `json:"rejected"`
}

type PushMessage struct {
	Notification struct {
		EventID string `json:"event_id"`
		Counts  struct {
			Unread int `json:"unread"`
		} `json:"counts"`
	} `json:"notification"`
	Timestamp int `json:"timestamp"`
}

type SharedState struct {
	mux     sync.RWMutex
	clients map[string]*ClientState
}

type ClientState struct {
	channel       chan *PushMessage
	lastNotify    *PushMessage
	hasLastNotify bool
	seenEventIds  map[string]bool
}

func (state *SharedState) IsClientKnown(token *string) bool {
	state.mux.RLock()
	known := state.clients[*token] != nil
	state.mux.RUnlock()
	return known
}

func (state *SharedState) SetLastNotify(token *string, notify *PushMessage) {
	state.mux.Lock()
	state.clients[*token].lastNotify = notify
	state.clients[*token].hasLastNotify = true
	state.mux.Unlock()
}

func (state *SharedState) GetLastNotify(token *string) *PushMessage {
	state.mux.RLock()
	lastNotify := state.clients[*token].lastNotify
	state.mux.RUnlock()
	return lastNotify
}

func (state *SharedState) IsEventIdKnown(token *string, eventId *string) bool {
	state.mux.RLock()
	isEventIdKnown := state.clients[*token].seenEventIds[*eventId]
	state.mux.RUnlock()
	return isEventIdKnown
}

func (state *SharedState) AddKnownEventId(token *string, eventId *string) {
	if len(*eventId) > 0 {
		state.mux.Lock()
		state.clients[*token].seenEventIds[*eventId] = true
		state.mux.Unlock()
	}
}

func (state *SharedState) SendNotifyToClientChannel(token *string, notify *PushMessage) {
	channel := state.clients[*token].channel

	tokenHash := hash(token)
	if channel == nil {
		log.Printf("%d has no active channel\n", tokenHash)
		return
	}

	l := len(channel)
	if l > 0 {
		log.Printf("%d has queued up %d messages in channel\n", tokenHash, l)
	}

	select {
	case channel <- notify:
	default:
		log.Printf("%d channel is full, drop message!\n", tokenHash)
	}
}
func (state *SharedState) NewClient(token *string) {
	tokenHash := hash(token)

	state.mux.Lock()
	known := state.clients[*token] != nil
	if known {
		log.Printf("%d channel will be replaced!\n", tokenHash)
		state.clients[*token].channel = make(chan *PushMessage, 1000)
	} else {
		log.Printf("%d is new client(#%d)\n", tokenHash, len(state.clients))
		state.clients[*token] = &ClientState{
			channel:      make(chan *PushMessage, 1000),
			lastNotify:   nil,
			seenEventIds: make(map[string]bool),
		}
	}
	state.mux.Unlock()
}

func (state *SharedState) GetClientChannel(token *string) (chan *PushMessage, bool) {
	state.mux.RLock()
	channel := state.clients[*token].channel
	state.mux.RUnlock()
	return channel, channel != nil
}

func (state *SharedState) RemoveClientChannel(token *string) {
	state.mux.Lock()
	state.clients[*token].channel = nil
	state.mux.Unlock()
}

func RemoveLineBreaks(text string) string {
	return strings.ReplaceAll(strings.ReplaceAll(text, "\n", ""), "\r", "")
}

func (notify *NotifyRequest) ToPushMessage(timestamp int) *PushMessage {
	msg := new(PushMessage)
	msg.Timestamp = timestamp
	msg.Notification.EventID = notify.Notification.EventID
	msg.Notification.Counts.Unread = notify.Notification.Counts.Unread
	return msg
}
