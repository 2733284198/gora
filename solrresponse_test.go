package gora

import (
	"testing"
)

func TestPopulateResponse(t *testing.T) {
	data := make(map[string]interface{})
	data["bad"] = true

	_, err := PopulateResponse(data)
	if err != ErrNoResponseHeader {
		t.Errorf("Expected NoResponseHeaderErr, got %v", err)
	}

	data = make(map[string]interface{})
	data["responseHeader"] = make(map[string]interface{})

	_, err = PopulateResponse(data)
	if err != ErrInvalidHeader {
		t.Errorf("Expected InvalidHeaderErr, got %v", err)
	}

	header := make(map[string]interface{})
	header["status"] = float64(7)
	header["QTime"] = float64(4)

	data = make(map[string]interface{})
	data["responseHeader"] = header

	response, err := PopulateResponse(data)
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}

	if response.Status != 7 {
		t.Errorf("Expected status of 7, got %d", response.Status)
	}

	if response.QTime != 4 {
		t.Errorf("Expected QTime of 4, got %d", response.QTime)
	}
}

func TestFromHttpResponse(t *testing.T) {
	raw := []byte(`{not valid json}`)
	_, err := SolrResponseFromHTTPResponse(raw)
	if err == nil {
		t.Error("Expected json unmarshal error")
	}

	raw = []byte(`{"responseHeader": {}}`)
	_, err = SolrResponseFromHTTPResponse(raw)
	if err != ErrInvalidHeader {
		t.Errorf("Expected InvalidHeaderErr, got %s", err)
	}

	raw = []byte(`{
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

	response, err := SolrResponseFromHTTPResponse(raw)
	if err != nil {
		t.Errorf("Unexpected error %s", err)
	}

	if len(response.Response.Docs) != 1 {
		t.Errorf("Expected 1 doument, found %d", len(response.Response.Docs))
	}

	raw = []byte(`{
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
		    "numFound": 0,
		    "start": 0,
		    "docs": []
        }
    }`)

	response, err = SolrResponseFromHTTPResponse(raw)
	if err != nil {
		t.Errorf("Unexpected error %s", err)
	}

	if len(response.Response.Docs) != 0 {
		t.Errorf("Expected 0 doument, found %d", len(response.Response.Docs))
	}

}
