package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hphuc15/mqttOQ/client"
	"github.com/hphuc15/mqttOQ/packet"
	"github.com/hphuc15/mqttOQ/transport"
)

func main() {
	// Configure client connection settings
	cfg := client.ClientConfig{
		Broker: client.ClientBroker{
			Address: "127.0.0.1:14567",
		},
		Credentials: client.ClientCredentials{
			ClientID: "example-quic-client",
		},
		Session: client.ClientSession{
			KeepAlive: 60,
		},
		Network: client.ClientNetwork{
			AutoReconnect:        true,
			ReconnectInterval:    1 * time.Second,
			MaxReconnectInterval: 30 * time.Second,
			ConnectTimeout:       5 * time.Second,
			Enable0RTT:           false,
			EnableMultiStream:    false,
		},
		TLSConfig: transport.NewClientTLSConfig(true, "mqtt"),

		// Lifecycle callbacks
		OnConnect: func(c *client.Client) {
			fmt.Printf("\n[Callback] Connected successfully (client ID: %s)\n", c.Config().Credentials.ClientID)
		},
		OnConnectionLost: func(c *client.Client, err error) {
			fmt.Printf("\n[Callback] Connection lost: %v\n", err)
		},
		OnReconnect: func(c *client.Client, attempt int) {
			fmt.Printf("\n[Callback] Reconnecting (attempt %d)...\n", attempt)
		},
	}

	c := client.NewClient(cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fmt.Println("Connecting to MQTT broker over QUIC at 127.0.0.1:14567...")
	if err := c.Connect(ctx); err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}

	// Subscribe to topic filter with QoS 1
	subTopic := "sensors/temperature"
	fmt.Printf("Subscribing to topic '%s' with QoS 1...\n", subTopic)
	err := c.Subscribe(ctx, subTopic, byte(packet.QoS1), func(client *client.Client, msg client.Message) {
		fmt.Printf("[Received] Topic: %s | Payload: %s | QoS: %d\n",
			msg.Topic, string(msg.Payload), msg.QoS)
	})
	if err != nil {
		log.Fatalf("Subscribe failed: %v", err)
	}
	fmt.Println("Subscribed successfully.")

	// Listen on system interrupt signals (Ctrl+C)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	fmt.Println("\nClient is now active. Publishing simulated sensor telemetry every 3 seconds.")
	fmt.Println("You can check EMQX Dashboard (Status: Connected) or publish from EMQX Dashboard to 'sensors/temperature'.")
	fmt.Println("Press Ctrl+C to stop.")

	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	counter := 1
	for {
		select {
		case <-sigCh:
			fmt.Println("\nStopping client...")
			disconnectCtx, dCancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer dCancel()
			if err := c.Disconnect(disconnectCtx); err != nil {
				log.Printf("Disconnect warning: %v\n", err)
			} else {
				fmt.Println("Disconnected cleanly.")
			}
			return

		case t := <-ticker.C:
			payload := fmt.Sprintf("{\"seq\": %d, \"temp\": %.1f, \"time\": \"%s\"}",
				counter, 24.0+float64(counter%10)*0.5, t.Format("15:04:05"))

			fmt.Printf("[Publishing] QoS 1 -> %s\n", payload)
			pubCtx, pCancel := context.WithTimeout(context.Background(), 3*time.Second)
			err := c.Publish(pubCtx, subTopic, byte(packet.QoS1), false, []byte(payload))
			pCancel()

			if err != nil {
				log.Printf("Publish failed: %v\n", err)
			}
			counter++
		}
	}
}
