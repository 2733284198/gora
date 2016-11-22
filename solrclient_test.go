package gora

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"strings"
	"testing"
	"time"
)

// RewriteTransport is an http.RoundTripper that rewrites requests
// using the provided URL's Scheme and Host, and its Path as a prefix.
// The Opaque field is untouched.
// If Transport is nil, http.DefaultTransport is used
// http://stackoverflow.com/a/27894872/61980
type RewriteTransport struct {
	//	Dial                  net.Dial
	ResponseHeaderTimeout time.Duration
	Transport             http.RoundTripper
	URL                   *url.URL
}

func (t RewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// note that url.URL.ResolveReference doesn't work here
	// since t.u is an absolute url
	req.URL.Scheme = t.URL.Scheme
	req.URL.Host = t.URL.Host
	req.URL.Path = path.Join(t.URL.Path, req.URL.Path)
	rt := t.Transport
	if rt == nil {
		rt = http.DefaultTransport
	}

	return rt.RoundTrip(req)
}

func createTestServer(testData io.Reader, expectedMethod string) (*httptest.Server, *HttpSolrClient) {
	testBuf, _ := ioutil.ReadAll(testData)
	testData.Read(testBuf)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := bytes.NewBuffer(testBuf)
		if !strings.HasSuffix(r.URL.String(), expectedMethod) {
			http.NotFound(w, r)
			return
		}
		io.Copy(w, buf)
	})

	server := httptest.NewServer(handler)
	testUrl, _ := url.Parse(server.URL)

	client := http.Client{
		Transport: RewriteTransport{
			URL: testUrl,
		},
	}

	solrClient := NewHttpSolrClient(server.URL, "core").(*HttpSolrClient)
	solrClient.client = &client

	// Return our test server and a client which points to it
	return server, solrClient
}

func TestQuery(t *testing.T) {
	expected := bytes.NewBufferString(`{
		  "responseHeader": {
		    "status": 0,
		    "QTime": 72,
		    "params": {
		      "q": "*:*",
		      "indent": "true",
		      "wt": "json",
		      "_": "1456851532479"
		    }
		  },
		  "response": {
		    "numFound": 21,
		    "start": 0,
		    "docs": [
		      {
		        "id": "My Id",
		        "greeting": "你好",
		        "list": [
		          "Jedná se o delší položka"
		        ],
		        "_version_": 1525161299814645800
		      }
  	        ]
        }
    }`)

	server, client := createTestServer(expected, "/select")
	defer server.Close()

	solrQuery := NewSolrQuery("*:*", 0, 100, nil, nil, nil, "/select")
	resp, retry := client.Execute(solrQuery)
	if retry {
		t.Fatalf("Should not have to retry a valid job")
	}

	if resp != nil && resp.Response.NumFound != 21 {
		t.Errorf("Expected 21 documents, found %d", resp.Response.NumFound)
	}
}

func TestTestConnection(t *testing.T) {
	expected := bytes.NewBufferString(`{}`)
	server, client := createTestServer(expected, "/update")
	defer server.Close()

	working := client.TestConnection()
	if !working {
		t.Error("Connection should be working")
	}

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	server = httptest.NewServer(handler)
	testUrl, _ := url.Parse("http://127.0.0.2:1/")

	httpClient := http.Client{
		Transport: RewriteTransport{
			URL: testUrl,
		},
	}

	solrClient := NewHttpSolrClient(server.URL, "core").(*HttpSolrClient)
	solrClient.client = &httpClient
	working = solrClient.TestConnection()
	if working {
		t.Error("Connection should not be working")
	}

}

func TestBadDecode(t *testing.T) {
	expected := bytes.NewBufferString(`{
		  "responseHeader": {
		    "status": 0,
		    "QTime": 72,
    }`)

	server, client := createTestServer(expected, "/update")
	defer server.Close()

	solrQuery := NewSolrQuery("*:*", 0, 100, nil, nil, nil, "/update")
	_, retry := client.Execute(solrQuery)
	if retry {
		t.Error("We should not retry this job")
	}
}
