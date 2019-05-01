package server

import (
	"bufio"
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"queries"
	"strconv"
	"time"

	"github.com/golang/glog"
)

var (
	listenPort = flag.String("listen-port", "8080", "port to listen on")
	data_path  = flag.String("data-path", "data", "directory to log data to")
)

type BlsReq struct {
	Year string `json:"year"`
	//use the same struct for query and fetching
	//incase we want to allow fetching the most recent
	//query for that year rather than an exact match
	QueryId string `json:"queryid"`
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
	http.HandleFunc("/bigquery_example_list", queryListHandler(&bls))
	http.HandleFunc("/bigquery_example_fetch", queryFetchHandler(&bls))

	if err := http.ListenAndServe(":"+*listenPort, nil); err != nil {
		log.Fatal(err)
	}
}

func blsHandler(b *queries.Bls) http.HandlerFunc {

	return func(w http.ResponseWriter, r *http.Request) {
		sCode, msg := ValidateJsonPost(r)
		if sCode != http.StatusOK {
			http.Error(w, msg, sCode)
		}
		//parse the body
		br, err := ParseRequestPost(r)
		if err != nil {
			glog.Error(err)
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("Error parsing json"))
			return
		}

		if br.Year == "" {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("Error parsing json"))
		}
		//validate that we have an actual number provided for the year
		yr, err := strconv.Atoi(br.Year)
		if err != nil || yr < 1900 || yr > time.Now().Year() {
			http.Error(w, "No queryid provided", http.StatusBadRequest)
			return
		}

		//use the nanos to create a uniq filename
		timeStr := strconv.FormatInt(time.Now().UnixNano(), 10)
		basename := br.Year + "-" + timeStr
		filename := basename + ".json"
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
		w.Write([]byte("{\"queryid\":\"" + basename + "\"}"))
	}
}

func queryFetchHandler(b *queries.Bls) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sCode, msg := ValidateJsonPost(r)
		if sCode != http.StatusOK {
			http.Error(w, msg, sCode)
		}

		//parse the body
		br, err := ParseRequestPost(r)

		if err != nil {
			glog.Error(err)
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("Error parsing json"))
			return
		}

		if br.QueryId == "" {
			http.Error(w, "No queryid provided", http.StatusBadRequest)
			return
		}

		queryFilename := b.DataPath + "/" + br.QueryId + ".json"
		data, err := getQueryData(queryFilename)
		if err != nil {
			//report as a generic server error
			http.Error(w, "Query data not available",
				http.StatusNoContent)

			//log the actual reason
			glog.Error(err)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, err = data.WriteTo(w)

		glog.Error(err)
	}
}

func queryListHandler(b *queries.Bls) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		list, err := getQueryList(b.DataPath)
		if err != nil {
			//report as a generic server error
			http.Error(w, http.StatusText(http.StatusInternalServerError),
				http.StatusInternalServerError)

			//log the actual reason
			glog.Error(err)
			return
		}
		j, err := json.Marshal(list)
		if err != nil {
			//report as a generic server error
			http.Error(w, http.StatusText(http.StatusInternalServerError),
				http.StatusInternalServerError)

			//log the actual reason
			glog.Error(err)
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write(j)
	}

}

func ValidateJsonPost(r *http.Request) (int, string) {
	//make sure its a POST
	if r.Method != http.MethodPost {
		return http.StatusMethodNotAllowed, http.StatusText(http.StatusMethodNotAllowed)
	}

	//check for the correct content type
	contentType := r.Header.Get("Content-type")
	if contentType != "application/json" {
		return http.StatusBadRequest, "Invalid content type"
	}

	return http.StatusOK, ""
}

func ParseRequestPost(r *http.Request) (*BlsReq, error) {
	var br BlsReq
	dec := json.NewDecoder(r.Body)
	err := dec.Decode(&br)
	if err != nil {
		return nil, err
	}
	return &br, nil
}

func getQueryList(path string) ([]string, error) {
	ids := []string{}

	files, err := ioutil.ReadDir(path)
	if err != nil {
		return ids, err
	}

	for _, file := range files {
		ext := filepath.Ext(file.Name())
		if ext == ".json" {
			ids = append(ids, file.Name()[0:len(file.Name())-5])
		}
	}
	return ids, nil
}

func getQueryData(filePath string) (*bufio.Reader, error) {
	_, err := os.Stat(filePath)
	if err == nil {
		file, err := os.Open(filePath)
		if err != nil {
			return nil, err
		}
		return bufio.NewReader(file), nil
	}
	return nil, err
}
