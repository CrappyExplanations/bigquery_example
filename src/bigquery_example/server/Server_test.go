package server

import (
	"flag"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"queries"
	"reflect"
	"strings"
	"testing"
)

func Test_startServer(t *testing.T) {
	assert := assert.New(t)

	if !flag.Parsed() {
		flag.Parse()
	}

	bls := queries.Bls{"test_project", *data_path}

	srv := httptest.NewServer(blsHandler(&bls))
	defer srv.Close()

	//try get requeset (should be post)
	resp, _ := http.Get(srv.URL)
	assert.Equal(resp.StatusCode, http.StatusMethodNotAllowed)

	//try post with wrong content type
	resp, _ = http.Post(srv.URL, "image/jpeg", nil)
	assert.Equal(resp.StatusCode, http.StatusBadRequest)

	//try post with no body
	resp, _ = http.Post(srv.URL, "application/json", nil)
	assert.Equal(resp.StatusCode, http.StatusInternalServerError)

	//don't populate a year field
	buf := strings.NewReader("{}")
	resp, _ = http.Post(srv.URL, "application/json", buf)
	assert.Equal(resp.StatusCode, http.StatusInternalServerError)

	//year field has no value
	buf = strings.NewReader("{\"year\":\"\"}")
	resp, _ = http.Post(srv.URL, "application/json", buf)
	assert.Equal(resp.StatusCode, http.StatusInternalServerError)

	//garbage year
	buf = strings.NewReader("{\"year\":\"abc\"}")
	resp, _ = http.Post(srv.URL, "application/json", buf)
	assert.Equal(resp.StatusCode, http.StatusBadRequest)

	//year to low
	buf = strings.NewReader("{\"year\":\"1\"}")
	resp, _ = http.Post(srv.URL, "application/json", buf)
	assert.Equal(resp.StatusCode, http.StatusBadRequest)

	//year to high
	buf = strings.NewReader("{\"year\":\"3000\"}")
	resp, _ = http.Post(srv.URL, "application/json", buf)
	assert.Equal(resp.StatusCode, http.StatusBadRequest)
}

func Test_getQueryList(t *testing.T) {
	assert := assert.New(t)
	dir, err := ioutil.TempDir("", "bigquery_example")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	bls := queries.Bls{"test_project", dir}

	srv := httptest.NewServer(queryListHandler(&bls))
	defer srv.Close()

	resp, _ := http.Get(srv.URL)
	assert.Equal(resp.StatusCode, http.StatusOK)
	respBody, _ := ioutil.ReadAll(resp.Body)
	assert.True(reflect.DeepEqual([]byte("[]"), respBody), "List should be empty")

	//Add some files and make sure they show up
	expected := "[\"2016-1234\",\"2017-1234\",\"2017-1235\"]"
	ioutil.WriteFile(dir+"/2016-1234.json", []byte("{2016-1234}"), 0644)
	ioutil.WriteFile(dir+"/2017-1234.json", []byte("{2017-1234}"), 0644)
	ioutil.WriteFile(dir+"/2017-1235.json", []byte("{2017-1235}"), 0644)
	ioutil.WriteFile(dir+"/2017-1235.png", []byte("{png}"), 0644)

	resp, _ = http.Get(srv.URL)
	assert.Equal(resp.StatusCode, http.StatusOK)
	respBody, _ = ioutil.ReadAll(resp.Body)
	assert.True(reflect.DeepEqual([]byte(expected), respBody), "Problem with list values")

}

func Test_queryFetch(t *testing.T) {
	assert := assert.New(t)
	dir, err := ioutil.TempDir("", "bigquery_example")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	bls := queries.Bls{"test_project", dir}

	//add some files to our temp directory
	ioutil.WriteFile(dir+"/2016-1234.json", []byte("{2016-1234}"), 0644)
	ioutil.WriteFile(dir+"/2017-1234.json", []byte("{2017-1234}"), 0644)

	srv := httptest.NewServer(queryFetchHandler(&bls))
	defer srv.Close()

	//try get requeset (should be post)
	resp, _ := http.Get(srv.URL)
	assert.Equal(resp.StatusCode, http.StatusMethodNotAllowed)

	//try post with wrong content type
	resp, _ = http.Post(srv.URL, "image/jpeg", nil)
	assert.Equal(resp.StatusCode, http.StatusBadRequest)

	//try post with no body
	resp, _ = http.Post(srv.URL, "application/json", nil)
	assert.Equal(resp.StatusCode, http.StatusInternalServerError)

	//don't populate a queryid field
	buf := strings.NewReader("{}")
	resp, _ = http.Post(srv.URL, "application/json", buf)
	assert.Equal(resp.StatusCode, http.StatusBadRequest)

	//queryid field has no value
	buf = strings.NewReader("{\"queryid\":\"\"}")
	resp, _ = http.Post(srv.URL, "application/json", buf)
	assert.Equal(resp.StatusCode, http.StatusBadRequest)

	//query for invalid id
	buf = strings.NewReader("{\"queryid\":\"2015-1234\"}")
	resp, _ = http.Post(srv.URL, "application/json", buf)
	assert.Equal(resp.StatusCode, http.StatusNoContent)

	//query for a valid id
	buf = strings.NewReader("{\"queryid\":\"2017-1234\"}")
	resp, _ = http.Post(srv.URL, "application/json", buf)
	assert.Equal(resp.StatusCode, http.StatusOK)

	expected := "{2017-1234}"
	respBody, _ := ioutil.ReadAll(resp.Body)
	assert.True(reflect.DeepEqual([]byte(expected), respBody), "Problem with fetched data")
}
