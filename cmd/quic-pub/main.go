package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/hphuc15/mqttOQ/client"
	"github.com/hphuc15/mqttOQ/transport"
)

func main() {
	broker := flag.String("broker", "127.0.0.1:14567", "Broker address (host:port)")
	topic := flag.String("topic", "test/topic", "MQTT topic to publish to")
	message := flag.String("m", "Hello MQTT over QUIC", "Message payload to publish")
	qos := flag.Int("qos", 1, "Quality of Service level (0, 1, 2)")
	retain := flag.Bool("retain", false, "Set MQTT retain flag")
	clientID := flag.String("client-id", "", "MQTT Client ID (default auto-generated)")
	count := flag.Int("count", 1, "Number of messages to publish")
	interval := flag.Duration("interval", 1*time.Second, "Delay between messages when count > 1")
	insecure := flag.Bool("insecure", true, "Skip TLS certificate verification")
	timeout := flag.Duration("timeout", 5*time.Second, "Connect timeout")
	flag.Parse()

	if *topic == "" {
		log.Fatal("Error: -topic flag cannot be empty")
	}
	if *qos < 0 || *qos > 2 {
		log.Fatalf("Error: invalid -qos %d (must be 0, 1, or 2)", *qos)
	}
	if *count < 1 {
		log.Fatalf("Error: -count must be at least 1")
	}

	cid := *clientID
	if cid == "" {
		cid = fmt.Sprintf("quic-pub-%d", time.Now().UnixNano()%1000000)
	}

	cfg := client.ClientConfig{
		Broker: client.ClientBroker{
			Address: *broker,
		},
		Credentials: client.ClientCredentials{
			ClientID: cid,
		},
		Session: client.ClientSession{
			KeepAlive: 60,
		},
		Network: client.ClientNetwork{
			ConnectTimeout: *timeout,
		},
		TLSConfig: transport.NewClientTLSConfig(*insecure, "mqtt"),
	}

	c := client.NewClient(cfg)

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	fmt.Printf("Connecting to QUIC broker at %s (ClientID: %s)...\n", *broker, cid)
	if err := c.Connect(ctx); err != nil {
		log.Fatalf("Connect failed: %v", err)
	}
	defer func() {
		disconnectCtx, dCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer dCancel()
		_ = c.Disconnect(disconnectCtx)
	}()

	fmt.Printf("Publishing %d message(s) to '%s' with QoS %d (retain=%v)...\n", *count, *topic, *qos, *retain)

	for i := 1; i <= *count; i++ {
		payload := *message
		if *count > 1 {
			payload = fmt.Sprintf("%s [#%d]", *message, i)
		}

		pubCtx, pCancel := context.WithTimeout(context.Background(), *timeout)
		err := c.Publish(pubCtx, *topic, byte(*qos), *retain, []byte(payload))
		pCancel()

		if err != nil {
			log.Printf("Failed to publish message #%d: %v\n", i, err)
		} else {
			fmt.Printf("[%s] Sent #%d: %s\n", time.Now().Format("15:04:05"), i, payload)
		}

		if i < *count && *interval > 0 {
			time.Sleep(*interval)
		}
	}

	fmt.Println("Publishing completed cleanly.")
	os.Exit(0)
}
