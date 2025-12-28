package main

import (
	"fmt"
	"log"

	"tidesdb"
)

func main() {
	db, err := tidesdb.Open("data", nil)
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
