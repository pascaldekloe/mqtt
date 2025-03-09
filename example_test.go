package mqtt_test

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/pascaldekloe/mqtt"
	"github.com/pascaldekloe/mqtt/mqtttest"
)

// Publish is a method from mqtt.Client.
var Publish func(quit <-chan struct{}, message []byte, topic string) error

// PublishAtLeastOnce is a method from mqtt.Client.
var PublishAtLeastOnce func(message []byte, topic string) (ack <-chan error, err error)

// Subscribe is a method from mqtt.Client.
var Subscribe func(quit <-chan struct{}, topicFilters ...string) error

// Online is a method from mqtt.Client.
var Online func() <-chan struct{}

func init() {
	PublishAtLeastOnce = mqtttest.NewPublishExchangeStub(nil)
	Subscribe = mqtttest.NewSubscribeStub(nil)
	Online = func() <-chan struct{} { return nil }
}

// Some brokers permit authentication with TLS client certificates.
func ExampleNewTLSDialer_clientCertificate() {
	certPEM := []byte(`-----BEGIN CERTIFICATE-----
MIIBhTCCASugAwIBAgIQIRi6zePL6mKjOipn+dNuaTAKBggqhkjOPQQDAjASMRAw
DgYDVQQKEwdBY21lIENvMB4XDTE3MTAyMDE5NDMwNloXDTE4MTAyMDE5NDMwNlow
EjEQMA4GA1UEChMHQWNtZSBDbzBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABD0d
7VNhbWvZLWPuj/RtHFjvtJBEwOkhbN/BnnE8rnZR8+sbwnc/KhCk3FhnpHZnQz7B
5aETbbIgmuvewdjvSBSjYzBhMA4GA1UdDwEB/wQEAwICpDATBgNVHSUEDDAKBggr
BgEFBQcDATAPBgNVHRMBAf8EBTADAQH/MCkGA1UdEQQiMCCCDmxvY2FsaG9zdDo1
NDUzgg4xMjcuMC4wLjE6NTQ1MzAKBggqhkjOPQQDAgNIADBFAiEA2zpJEPQyz6/l
Wf86aX6PepsntZv2GYlA5UpabfT2EZICICpJ5h/iI+i341gBmLiAFQOyTDT+/wQc
6MF9+Yw1Yy0t
-----END CERTIFICATE-----`)
	keyPEM := []byte(`-----BEGIN EC PRIVATE KEY-----
MHcCAQEEIIrYSSNQFaA2Hwf1duRSxKtLYX5CB04fSeQ6tF1aY/PuoAoGCCqGSM49
AwEHoUQDQgAEPR3tU2Fta9ktY+6P9G0cWO+0kETA6SFs38GecTyudlHz6xvCdz8q
EKTcWGekdmdDPsHloRNtsiCa697B2O9IFA==
-----END EC PRIVATE KEY-----`)

	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		log.Fatal(err)
	}
	mqtt.NewTLSDialer("tcp", "mq1.example.com:8883", &tls.Config{
		Certificates: []tls.Certificate{cert},
	})
	// Output:
}

// It is good practice to install the client from main.
func ExampleClient_setup() {
	client, err := mqtt.VolatileSession("demo-client", &mqtt.Config{
		Dialer:       mqtt.NewDialer("tcp", "localhost:1883"),
		PauseTimeout: 4 * time.Second,
	})
	if err != nil {
		log.Fatal("exit on broken setup: ", err)
	}

	// launch read-routine
	go func() {
		var big *mqtt.BigMessage
		for {
			message, topic, err := client.ReadSlices()
			switch {
			case err == nil:
				// do something with inbound message
				log.Printf("📥 %q: %q", topic, message)

			case errors.As(err, &big):
				log.Printf("📥 %q: %d byte message omitted", big.Topic, big.Size)

			case errors.Is(err, mqtt.ErrClosed):
				log.Print(err)
				return // terminated

			case mqtt.IsConnectionRefused(err):
				log.Print(err) // explains rejection
				// mqtt.ErrDown for a while
				time.Sleep(15 * time.Minute)

			default:
				log.Print("broker unavailable: ", err)
				// mqtt.ErrDown during backoff
				time.Sleep(2 * time.Second)
			}
		}
	}()

	// Install each method in use as a package variable. Such setup is
	// compatible with the tools from the mqtttest subpackage.
	Publish = client.Publish
	// Output:
}

// Demonstrates all error scenario and the respective recovery options.
func ExampleClient_PublishAtLeastOnce_critical() {
	for {
		exchange, err := PublishAtLeastOnce([]byte("🍸🆘"), "demo/alert")
		switch {
		case err == nil:
			fmt.Println("alert submitted…")
			break

		case mqtt.IsDeny(err), errors.Is(err, mqtt.ErrClosed):
			fmt.Println("🚨 alert not send:", err)
			return

		case errors.Is(err, mqtt.ErrMax):
			fmt.Println("⚠️ alert submission hold-up:", err)
			time.Sleep(time.Second / 4)
			continue

		default:
			fmt.Println("⚠️ alert submission blocked on persistence malfunction:", err)
			time.Sleep(4 * time.Second)
			continue
		}

		for err := range exchange {
			if errors.Is(err, mqtt.ErrClosed) {
				fmt.Println("🚨 alert exchange suspended:", err)
				// An AdoptSession may continue the transaction.
				return
			}

			fmt.Println("⚠️ alert request transfer interrupted:", err)
		}
		fmt.Println("alert acknowledged ✓")
		break
	}

	// Output:
	// alert submitted…
	// alert acknowledged ✓
}

// The switch lists all possible outcomes of a SUBSCRIBE request.
func ExampleClient_Subscribe_scenario() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	for {
		err := Subscribe(ctx.Done(), "demo/+")
		switch {
		case err == nil:
			fmt.Println("subscribe confirmed by broker")
			return

		case errors.As(err, new(mqtt.SubscribeError)):
			fmt.Println("subscribe failed by broker")
			return

		case mqtt.IsDeny(err): // illegal topic filter
			panic(err) // unreachable for string literal

		case errors.Is(err, mqtt.ErrClosed):
			fmt.Println("no subscribe with closed client")
			return

		case errors.Is(err, mqtt.ErrCanceled):
			fmt.Println("no subscribe with quit before submision")
			return

		case errors.Is(err, mqtt.ErrAbandoned):
			fmt.Println("subscribe in limbo with quit after submission")
			return

		case errors.Is(err, mqtt.ErrDown):
			fmt.Println("no subscribe without connection")
			select {
			case <-Online():
				fmt.Println("subscribe retry with new connection")
			case <-ctx.Done():
				fmt.Println("subscribe expired before reconnect")
				return
			}

		case errors.Is(err, mqtt.ErrSubmit), errors.Is(err, mqtt.ErrBreak):
			fmt.Println("subscribe in limbo with transit failure")
			select {
			case <-Online():
				fmt.Println("subscribe retry with new connection")
			case <-ctx.Done():
				fmt.Println("subscribe expired before reconnect")
				return
			}

		case errors.Is(err, mqtt.ErrMax):
			fmt.Println("no subscribe with too many requests")
			time.Sleep(time.Second) // backoff

		default: // unreachable
			fmt.Println("unknown subscribe state:", err)
			time.Sleep(time.Second) // backoff
		}
	}
	// Output:
	// subscribe confirmed by broker
}
