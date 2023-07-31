package esparser

import (
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"github.com/tidwall/gjson"
	"github.com/zilliztech/milvus-migration/core/type/estype"
	"strings"
)

const DOUBLE_QUOTA = `"`
const COMMA = ","
const JSON_START = `{"rows":[`
const JSON_END = "]}"
const SquareL = "["
const SquareR = "]"
const BraceL = "{"

// var BRACE_R = "}"

const JsonIdKey = `"_id":`
const _SOURCE = "_source"
const _ID = "_id"
const MILVUS_ID = _ID

func Next2JsonData(hits *gjson.Result, idx *estype.IdxCfg) []byte {
	return ParseHits(hits, COMMA, idx)
}

func First2JsonData(hits *gjson.Result, idx *estype.IdxCfg) []byte {
	//return ParseHits(hits, common.EMPTY)
	return ParseHits(hits, JSON_START, idx)
}

func StartCharacter() []byte {
	return []byte(JSON_START)
}

func ParseHits(hits *gjson.Result, startStr string, idx *estype.IdxCfg) []byte {
	var sb strings.Builder
	if len(startStr) > 0 {
		sb.WriteString(startStr)
	}
	arr := hits.Array()
	for n, obj := range arr {
		sb.WriteString(BraceL)
		if idx.InnerPkField == nil || idx.InnerPkField.Name == _ID {
			sb.WriteString(JsonIdKey)
			if idx.InnerPkType == nil || *idx.InnerPkType == entity.FieldTypeVarChar ||
				*idx.InnerPkType == entity.FieldTypeString {
				sb.WriteString(DOUBLE_QUOTA)
				sb.WriteString(obj.Get(_ID).String())
				sb.WriteString(DOUBLE_QUOTA)
				sb.WriteString(COMMA)
			} else {
				sb.WriteString(obj.Get(_ID).String())
				sb.WriteString(COMMA)
			}
		}
		source := obj.Get(_SOURCE).String()[1:]
		sb.WriteString(source)
		if n < len(arr)-1 {
			sb.WriteString(COMMA)
		}
	}
	return []byte(sb.String())
}

func get_IDVal(val string, milvusType *entity.FieldType) string {
	if *milvusType == entity.FieldTypeVarChar || *milvusType == entity.FieldTypeString {
		return DOUBLE_QUOTA + val + DOUBLE_QUOTA + COMMA
	} else {
		return val + COMMA
	}
}

func EndCharacter() []byte {
	return []byte(JSON_END)
}
