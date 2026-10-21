package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"time"

	"github.com/hphuc15/mqttOQ"
)

// import (
// 	"context"
// 	"crypto/tls"
// 	"fmt"
// 	"io"

// 	"github.com/quic-go/quic-go"
// )

// const alpn = "mqtt"

// func clientSend(message string) {

// }

// func runClient(serverAddr string, message string) error {
// 	var tlsConfig tls.Config = tls.Config{
// 		NextProtos:         []string{alpn},
// 		InsecureSkipVerify: true,
// 	}

// 	conn, e := quic.DialAddrEarly(context.Background(), serverAddr, &tlsConfig, nil)
// 	if e != nil {
// 		return e
// 	}
// 	defer conn.CloseWithError(0, "") /* Clean connection when runClient return value */

// 	stream, e := conn.OpenStreamSync(context.Background())
// 	if e != nil {
// 		return e
// 	}
// 	defer stream.Close()

// 	fmt.Printf("[Client] Sending message: \"%s\"\n", message)
// 	_, e = stream.Write([]byte(message))
// 	if e != nil {
// 		return e
// 	}

// 	buf := make([]byte, len(message))
// 	_, e = io.ReadFull(stream, buf)
// 	if e != nil {
// 		return e
// 	}
// 	fmt.Printf("[Client] Received response: \"%s\"\n", buf)

// 	return nil
// }

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c := mqttOQ.New("hp1", 60*time.Second, &tls.Config{InsecureSkipVerify: true})
	if err := c.Connect(ctx, "127.0.0.1:14567"); err != nil {
		log.Fatal(err)
	}
	fmt.Println("connected")
}
