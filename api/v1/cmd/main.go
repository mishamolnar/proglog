package main

import (
	"fmt"
	log_v1 "github.com/mishamolnar/proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

func main() {
	// Create a byte array with your 'hello world' message
	data := []byte{'h', 'e', 'l', 'l', 'o', ' ', 'w', 'o', 'r', 'l', 'd'}

	// Create an instance of the Record message
	record := &log_v1.Record{}
	record.Value = []byte{'h', 'e', 'l', 'l', 'o', ' ', 'w', 'o', 'r', 'l', 'd'}

	data, err := proto.Marshal(record)
	if err != nil {
		return
	}
	fmt.Println(data)
}

//func main() {
//	// Create a byte array with your 'hello world' message
//	data := []byte{'h', 'e', 'l', 'l', 'o', ' ', 'w', 'o', 'r', 'l', 'd'}
//
//	// Create an instance of the Record message
//	record := &log_v1.Record{}
//
//	// Unmarshal the data into the Record struct
//	err := proto.Unmarshal(data, record)
//	if err != nil {
//		fmt.Println("Error unmarshaling data:", err)
//		return
//	}
//
//	// Now you can access the fields of the Record struct
//	fmt.Printf("Value: %s\n", record.Value)
//	fmt.Printf("Offset: %d\n", record.Offset)
//}
