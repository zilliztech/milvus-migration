package esconvert

import (
	"github.com/tidwall/gjson"
	"strings"
)

var COMMA = ","
var JSON_START = `{"rows":[`
var JSON_END = "]}"
var SquareL = "["
var SquareR = "]"
var BraceL = "{"

// var BRACE_R = "}"

var JsonIdKey = `"id":"`
var _SOURCE = "_source"
var _ID = "_id"
var MILVUS_ID = "id"

func ToMilvus2Format(hits gjson.Result, first bool) []byte {
	var sb strings.Builder
	if first {
		sb.WriteString(JSON_START)
	} else {
		sb.WriteString(COMMA)
	}
	arr := hits.Array()
	for idx, obj := range arr {
		sb.WriteString(BraceL)
		sb.WriteString(JsonIdKey)
		sb.WriteString(obj.Get(_ID).String())
		sb.WriteString(`",`)
		source := obj.Get(_SOURCE).String()[1:]
		sb.WriteString(source)
		if idx < len(arr)-1 {
			sb.WriteString(COMMA)
		}
	}
	return []byte(sb.String())
}

func EndCharacter() []byte {
	return []byte(JSON_END)
}
