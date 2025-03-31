package types

import (
	"fmt"
	"net/url"
	"strings"
)

type IndexableDocument struct {
	Id        string
	IndexName string
	Data      map[string]any
}

func GetIndexableDoc(event Event) (*IndexableDocument, error) {
	if event.After == nil {
		return nil, nil
	}

	parts := strings.Split(event.After.Key, "/")
	if len(parts) < 5 {
		return nil, fmt.Errorf("invalid key: %s", event.After.Key)
	}

	cId := parts[1]
	resource := parts[3]

	object := event.After.Value.Object
	if object == nil {
		return nil, fmt.Errorf("object is nil for %s", cId)
	}

	id := fmt.Sprintf("%s", object["id"])
	if id == "" {
		return nil, fmt.Errorf("object id is empty for %+v", object)
	}

	if strings.Contains(id, "/") {
		id = url.PathEscape(id)
	}

	return &IndexableDocument{
		Id:        id,
		IndexName: getIndexName(resource),
		Data:      object,
	}, nil
}

func getIndexName(resource string) string {
	var indexName string
	switch resource {
	case "service":
		indexName = "kong-services"
	case "node":
		indexName = "kong-nodes"
	case "upstream":
		indexName = "kong-upstreams"
	case "store_event":
		indexName = "kong-events"
	default:
		indexName = "kong-default"
	}
	return indexName
}
