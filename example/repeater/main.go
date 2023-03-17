package main

import (
	"github.com/nla-is/datax-go"
	"log"
)

func main() {
	dx, err := datax.New()
	if err != nil {
		log.Fatalf("Error %v initializing DataX", err)
	}

	msg := map[string]interface{}{}
	for {
		stream, reference, err := dx.Next(&msg)
		if err != nil {
			log.Fatalf("Error %v receiving input message", err)
		}
		msg["repeated"] = true
		msg["fromStream"] = stream
		if err = dx.EmitWithReference(&msg, reference); err != nil {
			log.Fatalf("Error %v publishing message", err)
		}
	}
}
