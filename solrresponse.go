package gora

import (
	"encoding/json"
	"errors"
)

var (
	ErrNoResponseHeader = errors.New("Missing response header")
	ErrNoDocs           = errors.New("No documents found")
	ErrBadResponseType  = errors.New("Response is an unexpected type")
	ErrBadDocs          = errors.New("Documents is an unexpected type")
	ErrInvalidHeader    = errors.New("ResponseHeader was invalid")
)

// DocumentCollection represents a collection of solr documents
// and various other metrics
type DocumentCollection struct {
	Docs     []map[string]interface{}
	NumFound int
	Start    int
}

// SolrResponse represents a Solr response
type SolrResponse struct {
	Facets   map[string]interface{}
	Response *DocumentCollection
	Status   int
	QTime    int
	Error    error
}

// PopulateResponse will enumerate the fields of the passed map and create
// a SolrResponse. Only the "responseHeader" field is required. If there
// is a "response" field, it must contain "docs", even if empty.
func PopulateResponse(j map[string]interface{}) (*SolrResponse, error) {
	// look for a response element, bail if not present
	response_root := j
	response := response_root["response"]

	// begin Response creation
	r := SolrResponse{}

	// do status & qtime, if possible
	r_header, ok := response_root["responseHeader"].(map[string]interface{})
	if !ok {
		return nil, ErrNoResponseHeader
	}

	if status, ok := r_header["status"]; ok {
		r.Status = int(status.(float64))
	} else {
		return nil, ErrInvalidHeader
	}

	if qtime, ok := r_header["QTime"]; ok {
		r.QTime = int(qtime.(float64))
	} else {
		return nil, ErrInvalidHeader
	}

	// now do docs, if they exist in the response
	if response != nil {
		responseMap, ok := response.(map[string]interface{})
		if !ok {
			return nil, ErrBadResponseType
		}

		docs, ok := responseMap["docs"]
		if !ok {
			return nil, ErrNoDocs
		}

		docsSlice, ok := docs.([]interface{})
		if !ok {
			return nil, ErrBadDocs
		}

		// the total amount of results, irrespective of the amount returned in the response
		num_found := int(responseMap["numFound"].(float64))

		// and the amount actually returned
		num_results := len(docsSlice)

		coll := DocumentCollection{}
		coll.NumFound = num_found

		ds := make([]map[string]interface{}, 0, num_results)

		for i := 0; i < num_results; i++ {
			document, ok := docsSlice[i].(map[string]interface{})
			if ok {
				ds = append(ds, document)
			}
		}

		coll.Docs = ds
		r.Response = &coll
	}

	// If facets exist, add them as well
	facets := response_root["facets"]
	if facets != nil {
		r.Facets = facets.(map[string]interface{})
	}

	return &r, nil
}

// SolrResponseFromHTTPResponse decodes an HTTP (Solr) response
func SolrResponseFromHTTPResponse(b []byte) (*SolrResponse, error) {
	var container map[string]interface{}

	err := json.Unmarshal(b, &container)
	if err != nil {
		return nil, err
	}

	resp, err := PopulateResponse(container)
	if err != nil {
		return nil, err
	}

	return resp, nil
}
