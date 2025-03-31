package types

type Event struct {
	Before    map[string]any `json:"before"`
	After     *After         `json:"after"`
	Op        string         `json:"op"`
	TimeStamp int64          `json:"ts_ms"`
}

type After struct {
	Key   string `json:"key"`
	Value Value  `json:"value"`
}

type Value struct {
	Type   int            `json:"type"`
	Object map[string]any `json:"object"`
}
