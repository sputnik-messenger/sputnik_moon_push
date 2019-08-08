package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
)

func makeHandleNotify(state SharedState) func(w http.ResponseWriter, r *http.Request) {

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
		strBytes, _ := json.Marshal(notifyRequest)
		log.Println(string(strBytes))

		for _, device := range notifyRequest.Notification.Devices {
			state.mux.Lock()
			channel := state.clients[device.Pushkey]
			if channel == nil {
				log.Println("no client connected")
			}
			state.mux.Unlock()

			if channel != nil {
				log.Println("send to channel")
				channel <- notifyRequest
			}
		}

		w.Header().Add("Content-Type", "text/json; charset=utf-8")
		err = json.NewEncoder(w).Encode(NotifyResponse{Rejected: []string{}})
		if err != nil {
			log.Println(err.Error())
		}

	}
}

func makeSendNotifications(state SharedState) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {

		tokens, ok := r.URL.Query()["token"]
		if !ok || len(tokens) == 0 || len(tokens[0]) == 0 {
			http.Error(w, "missing token", 400)
			return
		}

		w.Header().Set("Cache-Control", "no-cache");
		w.Header().Set("Content-Type", "text/event-stream");
		w.Header().Set("Transfer-Encoding", "identity")
		w.Header().Set("Connection", "keep-alive")

		token := tokens[0]
		log.Println("client with token " + token)

		state.mux.Lock()
		channel := state.clients[token]
		if channel != nil {
			log.Println("old channel found!! will be replaced!")
		}
		log.Println("new channel")
		channel = make(chan NotifyRequest, 1000)
		state.clients[token] = channel
		state.mux.Unlock()

		for {
			select {
			case <-r.Context().Done():
				log.Println("sse conncetion closed")
				return
			case notifyRequest := <-channel:
				r.Context().Done()
				log.Println("send data")

				strBytes, _ := json.Marshal(notifyRequest)
				jsonString := strings.ReplaceAll(string(strBytes), "\n", "")
				_, err := fmt.Fprintf(w, "data: "+jsonString,"\n")
				if err != nil {
					log.Println(err.Error())
				} else {
					if f, ok := w.(http.Flusher); ok {
						f.Flush()
					} else {
						log.Fatal("Can't do SSE without Flushing.");
					}
				}
				if err != nil {
					http.Error(w, err.Error(), 500)
					return
				}
			}
		}

	}
}

func main() {
	log.Println("starting server")

	sharedState := SharedState{
		clients: make(map[string]chan NotifyRequest),
	}

	http.HandleFunc("/push_gateway/_matrix/push/v1/notify", makeHandleNotify(sharedState))
	http.HandleFunc("/push_gateway/notifications", makeSendNotifications(sharedState))

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

type SharedState struct {
	mux     sync.Mutex
	clients map[string]chan NotifyRequest
}

type ClientState struct {
	channel          chan int
	lastNotification NotifyRequest
}
