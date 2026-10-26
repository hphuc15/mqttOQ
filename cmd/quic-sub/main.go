package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hphuc15/mqttOQ/client"
	"github.com/hphuc15/mqttOQ/transport"
)

func main() {
	broker := flag.String("broker", "127.0.0.1:14567", "Broker address (host:port)")
	topic := flag.String("topic", "test/#", "MQTT topic filter to subscribe to")
	qos := flag.Int("qos", 1, "Requested Quality of Service level (0, 1, 2)")
	clientID := flag.String("client-id", "", "MQTT Client ID (default auto-generated)")
	insecure := flag.Bool("insecure", true, "Skip TLS certificate verification")
	timeout := flag.Duration("timeout", 5*time.Second, "Connect timeout")
	flag.Parse()

	if *topic == "" {
		log.Fatal("Error: -topic flag cannot be empty")
	}
	if *qos < 0 || *qos > 2 {
		log.Fatalf("Error: invalid -qos %d (must be 0, 1, or 2)", *qos)
	}

	cid := *clientID
	if cid == "" {
		cid = fmt.Sprintf("quic-sub-%d", time.Now().UnixNano()%1000000)
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
			AutoReconnect:        true,
			ReconnectInterval:    1 * time.Second,
			MaxReconnectInterval: 30 * time.Second,
			ConnectTimeout:       *timeout,
		},
		TLSConfig: transport.NewClientTLSConfig(*insecure, "mqtt"),

		OnConnect: func(c *client.Client) {
			fmt.Printf("[Callback] Connected (ClientID: %s)\n", c.Config().Credentials.ClientID)
		},
		OnConnectionLost: func(c *client.Client, err error) {
			fmt.Printf("[Callback] Connection lost: %v\n", err)
		},
		OnReconnect: func(c *client.Client, attempt int) {
			fmt.Printf("[Callback] Reconnecting (attempt %d)...\n", attempt)
		},
	}

	c := client.NewClient(cfg)

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	fmt.Printf("Connecting to QUIC broker at %s (ClientID: %s)...\n", *broker, cid)
	if err := c.Connect(ctx); err != nil {
		log.Fatalf("Connect failed: %v", err)
	}

	subCtx, sCancel := context.WithTimeout(context.Background(), *timeout)
	defer sCancel()

	fmt.Printf("Subscribing to '%s' with QoS %d...\n", *topic, *qos)
	err := c.Subscribe(subCtx, *topic, byte(*qos), func(client *client.Client, msg client.Message) {
		retainTag := ""
		if msg.Retained {
			retainTag = " [retained]"
		}
		fmt.Printf("[%s] Topic: %s (QoS %d%s) -> %s\n",
			time.Now().Format("15:04:05"), msg.Topic, msg.QoS, retainTag, string(msg.Payload))
	})
	if err != nil {
		log.Fatalf("Subscribe failed: %v", err)
	}

	fmt.Printf("Subscribed successfully. Listening for incoming messages...\nPress Ctrl+C to exit.\n\n")

	// Wait for Ctrl+C signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	<-sigCh
	fmt.Println("\nStopping subscriber...")

	disconnectCtx, dCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer dCancel()
	_ = c.Disconnect(disconnectCtx)

	fmt.Println("Disconnected cleanly.")
}
