package server

import (
	"encoding/json"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"queries"
	"strconv"
	"time"

	"github.com/golang/glog"
)

var (
	listenPort = flag.String("listen-port", "8080", "port to listen on")
	data_path = flag.String("data-path", "data", "directory to log data to")
)

type BlsReq struct {
	Year string `json:"year"`
}

func StartServer() {
	if !flag.Parsed() {
		flag.Parse()
	}

	//if the data directory doesn't exist, create it
	if _, err := os.Stat(*data_path); os.IsNotExist(err) {
		os.Mkdir(*data_path, 0700)
	}

	bls := queries.NewBLS(*data_path)

	http.HandleFunc("/bigquery_example", blsHandler(&bls))
	if err := http.ListenAndServe(":"+*listenPort, nil); err != nil {
		log.Fatal(err)
	}
}

func blsHandler(b *queries.Bls) http.HandlerFunc {

	return func(w http.ResponseWriter, r *http.Request) {
		//make sure its a POST
		if r.Method != http.MethodPost {
			http.Error(w, http.StatusText(http.StatusMethodNotAllowed),
				http.StatusMethodNotAllowed)
			return
		}

		//check for the correct content type
		contentType := r.Header.Get("Content-type")
		if contentType != "application/json" {
			http.Error(w, "Invalid content type",
				http.StatusBadRequest)
			return
		}

		//parse the body
		br, err := ParseRequestPost(r)
		if err != nil {
			glog.Error(err)
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("Error parsing json"))
			return
		}

		//use the nanos to create a uniq filename
		timeStr := strconv.FormatInt(time.Now().UnixNano(), 10)
		filename := br.Year + "-" + timeStr + ".json"
		filepath := b.DataPath + "/" + filename
		f, err := os.OpenFile(filepath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			//report as a generic server error
			http.Error(w, http.StatusText(http.StatusInternalServerError),
				http.StatusInternalServerError)

			//log the actual reason
			glog.Error(err)
			return
		}

		//make the query and store it to the file provided
		err = b.QueryAndStore(f, br.Year)

		if err != nil {
			//report as a generic server error
			http.Error(w, http.StatusText(http.StatusInternalServerError),
				http.StatusInternalServerError)

			//log the actual reason
			glog.Error(err)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

func ParseRequestPost(r *http.Request) (*BlsReq, error) {
	var br BlsReq
	dec := json.NewDecoder(r.Body)
	err := dec.Decode(&br)
	if err != nil {
		return nil, err
	}

	if br.Year == "" {
	  return nil, errors.New("Year must be set")
	}

	return &br, nil
}
