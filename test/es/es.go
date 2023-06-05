package main

import (
	es7 "github.com/elastic/go-elasticsearch/v7"
	es8 "github.com/elastic/go-elasticsearch/v8"
	"log"
)

func main() {
	test_es7()
	test_es8()
}

func test_es7() {
	client, _ := es7.NewDefaultClient()
	//client.Scroll.
	log.Println(es7.Version)
	log.Println(client.Info())
}

func test_es8() {
	es8.NewDefaultClient()
}
