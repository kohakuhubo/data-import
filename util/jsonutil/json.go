package jsonutil

import "encoding/json"

func MapToString(obj map[string]interface{}) string {
	data, err := json.Marshal(obj)
	if err != nil {
		return ""
	}
	return string(data)
}
