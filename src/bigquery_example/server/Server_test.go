package server

import (
	"flag"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"queries"
	"strings"
	"testing"
)

func Test_startServer(t *testing.T) {
	assert := assert.New(t)
	bls := queries.Bls{"test_project", *data_path}

	if !flag.Parsed() {
		flag.Parse()
	}

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
}
