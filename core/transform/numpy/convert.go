// Copyright 2016 The npyio Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package npconvert

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/zilliztech/milvus-migration/core/common"
	"github.com/zilliztech/milvus-migration/internal/log"
)

var numpy_magic_head = []byte{'\x93', 'N', 'U', 'M', 'P', 'Y'}

var order = binary.LittleEndian

type npyHeader struct {
	Major byte // data file major version
	Minor byte // data file minor version
	Descr struct {
		Type    string // data type of array elements ('<i8', '<f4', ...)
		Fortran bool   // whether the array data is stored in Fortran-order (col-major)
		Shape   []int  // array shape (e.g. [2,3] a 2-rows, 3-cols array
	}
}

func newNpyHeader() npyHeader {
	return npyHeader{
		Major: 1,
		Minor: 0,
	}
}

func ConvertToNumpyHead(meta common.CMeta) ([]byte, error) {

	retBuf := new(bytes.Buffer)

	// magic
	retBuf.Write(numpy_magic_head)

	header := newNpyHeader()
	// major
	retBuf.Write([]byte{header.Major})

	// version
	retBuf.Write([]byte{header.Minor})

	dType, err := getDTypeByGoType(meta.Type)
	if err != nil {
		return nil, err
	}

	// shape
	headBuf := new(bytes.Buffer)
	fmt.Fprintf(headBuf, "{'descr': '%s', 'fortran_order': False, 'shape': %s, }",
		dType,
		shapeString(meta.Row-meta.DeleteOffset, meta.Dim),
	)

	var hdrSize int
	switch header.Major {
	case 1:
		hdrSize = 4 + len(numpy_magic_head)
	case 2:
		hdrSize = 6 + len(numpy_magic_head)
	default:
		msg := fmt.Sprintf("npy: invalid major version number (%d)", header.Major)
		log.Error(msg)
		return nil, errors.New(msg)
	}

	padding := (hdrSize + headBuf.Len() + 1) % 16
	headBuf.Write(bytes.Repeat([]byte{'\x20'}, padding))
	headBuf.Write([]byte{'\n'})

	// calculate headLength
	headBufLen := int64(headBuf.Len())
	switch header.Major {
	case 1:
		binary.Write(retBuf, order, uint16(headBufLen))
	case 2:
		binary.Write(retBuf, order, uint32(headBufLen))
	default:
		msg := fmt.Sprintf("npy: invalid major version number (%d) in headLength", header.Major)
		log.Error(msg)
		return nil, errors.New(msg)
	}

	// write shape head
	retBuf.Write(headBuf.Bytes())

	return retBuf.Bytes(), nil
}

func getDTypeByGoType(goType string) (string, error) {
	var dType string
	switch goType {
	case "float32":
		dType = "f4"
	case "float64":
		dType = "f8"
	case "int32":
		dType = "i4"
	case "int64":
		dType = "i8"
	default:
		msg := fmt.Sprintf("not support convert numpy data type yet, goType=%s", goType)
		log.Error(msg)
		return "", errors.New(msg)
	}

	return "<" + dType, nil
}

func shapeString(row int, dim int) string {
	if row == 0 && dim == 0 {
		return "()"
	}

	if dim == 0 {
		return fmt.Sprintf("(%d,)", row)
	}

	shape := []int{row, dim}
	var str []string
	for _, v := range shape {
		str = append(str, strconv.Itoa(v))
	}
	return fmt.Sprintf("(%s)", strings.Join(str, ", "))

}
