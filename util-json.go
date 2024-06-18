package ddbrepo

import (
	"encoding/json"
)

func JsonPretty[T any](t *T, params ...string) string {
	prefix, indent := "", "    "
	if len(params) >= 1 {
		prefix = params[0]
	}
	if len(params) >= 2 {
		indent = params[1]
	}
	bytes, _ := json.MarshalIndent(t, prefix, indent)
	return prefix + string(bytes)
}

func JsonLine[T any](t *T) string {
	bytes, _ := json.Marshal(t)
	return string(bytes)
}
