package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/apex/log"
	"github.com/davecgh/go-spew/spew"
	"github.com/gorilla/mux"
	"github.com/satori/go.uuid"

	"data-lake/datalakemsg"
)

type LogPostResponse struct {
	ID      string `json:"id"`
	Channel string `json:"channel"`
}

func PostV1Syslog(w http.ResponseWriter, r *http.Request) {

	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var msg datalakemsg.LogMessage
	//msg := new(datalakemsg.LogMessage)

	// read body with limit
	body, err := ioutil.ReadAll(io.LimitReader(r.Body, 1048576))
	if err != nil {
		log.Errorf("%s", err)
		panic(err)
	}
	if err := r.Body.Close(); err != nil {
		log.Errorf("%s", err)
		panic(err)
	}

	if err := json.Unmarshal(body, &msg); err != nil {
		log.Errorf("Could not decode JSON body of incoming message %s, error %s", body, err)
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.WriteHeader(422) // unprocessable entity
		if err := json.NewEncoder(w).Encode(err); err != nil {
			panic(err)
		}
		return
	}

	if msg.ID == "" {
		msg.ID = fmt.Sprintf("%s", uuid.Must(uuid.NewV4()))
	}
	if msg.TimeStamp == "" {
		now := time.Now()
		msg.TimeStamp = now.Format(time.RFC822)
	}

	go addMessageToQueue(msg, "incoming-v1-syslog")
	w.WriteHeader(http.StatusOK)
}

func PostV1SyslogChannel(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	params := mux.Vars(r)
	var msg datalakemsg.LogMessage

	// read body with limit
	body, err := ioutil.ReadAll(io.LimitReader(r.Body, 1048576))
	if err != nil {
		panic(err)
	}
	if err := r.Body.Close(); err != nil {
		panic(err)
	}

	if err := json.Unmarshal(body, &msg); err != nil {
		//w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.WriteHeader(422) // unprocessable entity
		if err := json.NewEncoder(w).Encode(err); err != nil {
			panic(err)
		}
	}

	addMessageToQueue(msg, params["channel"])
	w.WriteHeader(http.StatusOK)
}

func addMessageToQueue(msg datalakemsg.LogMessage, topic string) {
	payload := Payload{Topic: topic, Message: msg}

	work := Job{Payload: payload}
	spew.Dump(JobQueue)
	JobQueue <- work
	log.Infof("%s has been queued for topic %s", msg.Body, topic)
}
