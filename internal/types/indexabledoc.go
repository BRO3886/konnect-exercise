package types

import (
	"fmt"
	"strings"
)

type IndexableDocument struct {
	Id      string
	DeIndex bool
	Data    map[string]any
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

	var id string
	if _, ok := object["id"]; !ok {
		id = parts[4]
	} else {
		id = fmt.Sprintf("%s", object["id"])
	}

	if id == "" {
		return nil, fmt.Errorf("object id is empty for %+v", object)
	}

	id = strings.ReplaceAll(id, "/", "-")

	object["_metadata"] = map[string]any{
		"resource_type": resource,
		"cid":           cId,
	}

	return &IndexableDocument{
		Id:      id,
		DeIndex: event.Op == "d",
		Data:    object,
	}, nil
}
