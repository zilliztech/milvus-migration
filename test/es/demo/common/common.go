package common

import (
	"math/rand"
	"strconv"
)

func GetInsertValue(i int) *InsertValue {
	var bl bool
	if i%2 == 0 {
		bl = true
	}
	vector := make([]float32, 0, 512)
	for i = 0; i < 512; i++ {
		vector = append(vector, rand.Float32())
	}
	return &InsertValue{
		Text1: "text1" + strconv.Itoa(i),
		Keyw1: "keyxx" + strconv.Itoa(i),
		Long1: rand.Int63(),
		Int1:  rand.Int31(),
		Bl2:   bl,
		Doub1: rand.Float64(),
		Dvec:  vector,
	}
}

type InsertValue struct {
	Text1 string    `json:"text1"`
	Keyw1 string    `json:"keyw1"`
	Long1 int64     `json:"long1"`
	Int1  int32     `json:"int1"`
	Bl2   bool      `json:"bl2"`
	Doub1 float64   `json:"doub1"`
	Dvec  []float32 `json:"dvec"`
}
