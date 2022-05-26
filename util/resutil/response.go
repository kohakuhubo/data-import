package resutil

import (
	"encoding/json"
	"net/http"
)

var (
	SUCCESS        = &ErrorCode{0, "success!"}
	BUSINESS_ERROR = &ErrorCode{10010, "business error!"}
	SYSTEM_ERROR   = &ErrorCode{10001, "system error!"}
	TOKEN_ERROR    = &ErrorCode{10002, "token error!"}
	UNKNOWN_ERROR  = &ErrorCode{10003, "unknown error!"}
	RESPONSE_ERROR = &ErrorCode{10004, "response error!"}
)

type ErrorCode struct {
	code    int
	message string
}

func (e ErrorCode) getCode() int {
	return e.code
}

func (e ErrorCode) getMessage() string {
	return e.message
}

type ResponseEntity struct {
	Code    int         `json:"code"`
	Data    interface{} `json:"data"`
	Message string      `json:"message"`
}

func WriteJson(w http.ResponseWriter, res *ResponseEntity) {
	data, _ := json.Marshal(res)
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func Success(data interface{}) *ResponseEntity {
	return &ResponseEntity{
		Code:    SUCCESS.getCode(),
		Data:    data,
		Message: SUCCESS.getMessage(),
	}
}

func Error(errorCode *ErrorCode, message string) *ResponseEntity {

	if message == "" {
		message = errorCode.getMessage()
	}

	return &ResponseEntity{
		Code:    errorCode.getCode(),
		Message: message,
	}
}
