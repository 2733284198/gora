package gora

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
)

func TestSolrQuery(t *testing.T) {
	expectedParams := make(map[string]interface{})
	expectedParams["wt"] = "json"
	expectedParams["start"] = float64(0)
	expectedParams["rows"] = float64(0)

	expected := make(map[string]interface{})
	expected["query"] = "greeting:你好 AND date:1January2016"
	expected["params"] = expectedParams

	var result map[string]interface{}

	query := fmt.Sprintf("greeting:你好 AND date:1January2016")
	solrQuery := NewSolrQuery(query, 0, 0, nil, nil, nil, "select")

	err := json.Unmarshal(solrQuery.Bytes(), &result)
	if err != nil {
		t.Fatal("Unexpected error ", err)
	}

	if !reflect.DeepEqual(result, expected) {
		t.Error("Result was unexpected ", result)
	}
}

func TestSpatialQuery(t *testing.T) {
	expectedParams := make(map[string]interface{})
	expectedParams["wt"] = "json"
	expectedParams["fq"] = "{!bbox sfield=latlon}"
	expectedParams["d"] = "1.000000"
	expectedParams["pt"] = "1.230000,-4.560000"
	expectedParams["start"] = float64(0)
	expectedParams["rows"] = float64(0)

	expected := make(map[string]interface{})
	expected["query"] = "greeting:你好 AND date:1January2016"
	expected["params"] = expectedParams

	var result map[string]interface{}

	query := fmt.Sprintf("greeting:你好 AND date:1January2016")
	solrQuery := NewSolrSpatialQuery(query, "bbox", "latlon", 1.23, -4.56, 1, 0, 0, nil, nil, nil, "select")

	err := json.Unmarshal(solrQuery.Bytes(), &result)
	if err != nil {
		t.Fatal("Unexpected error ", err)
	}

	if !reflect.DeepEqual(result, expected) {
		t.Error("Result was unexpected ", result)
	}
}

func TestSolrUpdateQuery(t *testing.T) {
	expected := []byte(`{"add":{"doc":{"deeper":{"one":"one","two":"two"},"id":"test-id","int_list":[1,2,0,3],"nil":null,"string_list":["你好","Jedná se o delší položka",""]}}, "commit": {}}`)

	deeper := make(map[string]string)
	deeper["one"] = "one"
	deeper["two"] = "two"

	query := make(map[string]interface{})
	query["id"] = "test-id"
	query["string_list"] = []string{"你好", "Jedná se o delší položka", ""}
	query["int_list"] = []int{1, 2, 0, 3}
	query["deeper"] = deeper
	query["nil"] = nil

	solrQuery := NewSolrUpdateQuery(query)

	if bytes.Compare(expected, solrQuery.Bytes()) != 0 {
		t.Errorf("Found unexpected query data: %s", solrQuery.Bytes())
	}
}

func TestSolrBatchUpdateQuery(t *testing.T) {
	expected := []byte(`{"add":{"doc":{"deeper":{"one":"one","two":"two"},"id":"test-id","int_list":[1,2,0,3],"nil":null,"string_list":["你好","Jedná se o delší položka",""]}},"add":{"doc":{"deeper":{"one":"one","two":"two"},"id":"test-id2","int_list":[2,4,6,8],"nil":null,"string_list":["你好","Jedná se o delší položka",""]}}, "commit": {}}`)

	deeper := make(map[string]string)
	deeper["one"] = "one"
	deeper["two"] = "two"

	query := make(map[string]interface{})
	query["id"] = "test-id"
	query["string_list"] = []string{"你好", "Jedná se o delší položka", ""}
	query["int_list"] = []int{1, 2, 0, 3}
	query["deeper"] = deeper
	query["nil"] = nil

	nextQuery := make(map[string]interface{})
	nextQuery["id"] = "test-id2"
	nextQuery["string_list"] = []string{"你好", "Jedná se o delší položka", ""}
	nextQuery["int_list"] = []int{2, 4, 6, 8}
	nextQuery["deeper"] = deeper
	nextQuery["nil"] = nil

	queries := []map[string]interface{}{query, nextQuery}
	solrQuery := NewSolrBatchUpdateQuery(queries)

	if bytes.Compare(expected, solrQuery.Bytes()) != 0 {
		t.Errorf("Found unexpected query data: %s", solrQuery.Bytes())
	}
}

func TestSolrDeleteQuery(t *testing.T) {
	expected := []byte("{\"delete\":{\"query\":\"*:*\"}, \"commit\": {}}")
	query := NewSolrDeleteQuery("*:*")

	if bytes.Compare(expected, query.Bytes()) != 0 {
		t.Errorf("Found unexpected query data: %s", query.Bytes())
	}

}

func TestSolrBatchDeleteQuery(t *testing.T) {
	expected := []byte(`{"delete":["one","two","three"], "commit": {}}`)
	query := NewSolrBatchDeleteQuery([]string{"one", "two", "three"})

	if bytes.Compare(expected, query.Bytes()) != 0 {
		t.Errorf("Found unexpected query data: %s", query.Bytes())
	}
}
