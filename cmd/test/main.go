package main

import (
	"context"
	"crypto/tls"
	"log"
	"time"

	"github.com/hphuc15/mqttOQ"
)

func main() {
	// InsecureSkipVerify only because the test broker likely uses a self-signed cert.
	c := mqttOQ.NewClient("test-client", 60*time.Second, &tls.Config{InsecureSkipVerify: true})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := c.Connect(ctx, "127.0.0.1:14567"); err != nil {
		log.Fatal(err)
	}
	log.Println("CONNECT sent")
	time.Sleep(2 * time.Second) // keep the connection open long enough to observe the broker log
}
