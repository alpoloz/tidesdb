package main

import (
	"fmt"
	"log"

	"go.uber.org/zap"
	"tidesdb"
)

func main() {
	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		_ = logger.Sync()
	}()

	db, err := tidesdb.Open(logger, "data", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := db.Put("hello", []byte("world")); err != nil {
		log.Fatal(err)
	}

	value, err := db.Get("hello")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(value))

	if err := db.Delete("hello"); err != nil {
		log.Fatal(err)
	}
}
