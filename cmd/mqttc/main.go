// Package main provides a command-line utility.
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pascaldekloe/mqtt"
)

const messageMax = 256 * 1024 * 1024

// ANSI escape codes for markup.
const (
	bold   = "\x1b[1m"
	italic = "\x1b[3m"
	clear  = "\x1b[0m"
)

// Name of the invoked executable.
var name = os.Args[0]

var subscribeFlags []string

func init() {
	flag.Func("subscribe", "Listen with a topic `filter`. Inbound messages are printed to\n"+italic+"standard output"+clear+" until interrupted by a signal(3). Multiple\n"+bold+"-subscribe"+clear+" options may be applied together.", func(value string) error {
		subscribeFlags = append(subscribeFlags, value)
		return nil
	})
}

const generatedLabel = "generated"

var (
	publishFlag = flag.String("publish", "", "Send a message to a `topic`. The payload is read from "+italic+"standard\ninput"+clear+".")

	timeoutFlag = flag.Duration("timeout", 4*time.Second, "Network operation expiry.")
	netFlag     = flag.String("net", "tcp", "Select the network by `name`. Valid alternatives include tcp4,\ntcp6 and unix.")
	tlsFlag     = flag.Bool("tls", false, "Secure the connection with TLS.")
	serverFlag  = flag.String("server", "", "Use a specific server `name` with TLS")
	userFlag    = flag.String("user", "", "The user `name` may be used by the broker for authentication\nand/or authorization purposes.")
	passFlag    = flag.String("pass", "", "The `file` content is used as a password.")

	clientFlag = flag.String("client", generatedLabel, "Use a specific client `identifier`.")

	prefixFlag = flag.String("prefix", "", "Print a `string` before each inbound message.")
	suffixFlag = flag.String("suffix", "\n", "Print a `string` after each inbound message.")
	topicFlag  = flag.Bool("topic", false, "Print the respective topic of each inbound message.")
	quoteFlag  = flag.Bool("quote", false, "Print inbound topics and messages as quoted strings.")

	quietFlag   = flag.Bool("quiet", false, "Suppress all output to "+italic+"standard error"+clear+". Error reporting is\ndeduced to the exit code only.")
	verboseFlag = flag.Bool("verbose", false, "Produces more output to "+italic+"standard error"+clear+" for debug purposes.")
)

func parseConfig() (clientID string, config *mqtt.Config) {
	var addr string
	switch args := flag.Args(); {
	case len(args) == 0:
		printManual()
		os.Exit(2)

	case len(args) == 1:
		addr = args[0]

	default:
		log.Printf("%s: multiple address arguments %q", name, args)
		os.Exit(2)
	}
	if _, _, err := net.SplitHostPort(addr); err != nil {
		port := "1883"
		if *tlsFlag {
			port = "8883"
		}
		addr = net.JoinHostPort(addr, port)
	}

	clientID = *clientFlag
	if clientID == generatedLabel {
		clientID = "mqttc(1)-" + time.Now().In(time.UTC).Format(time.RFC3339Nano)
	}

	config = &mqtt.Config{
		WireTimeout: *timeoutFlag,
		UserName:    *userFlag,
	}
	if *passFlag != "" {
		bytes, err := os.ReadFile(*passFlag)
		if err != nil {
			log.Fatal(err)
		}
		config.Password = bytes
	}

	if *tlsFlag {
		config.Dialer = mqtt.NewTLSDialer(*netFlag, addr, &tls.Config{
			ServerName: *serverFlag,
		})
	} else {
		config.Dialer = mqtt.NewDialer(*netFlag, addr)
	}
	return
}

var exitStatus = make(chan int, 1)

func setExitStatus(code int) {
	select {
	case exitStatus <- code:
	default: // exit status already defined
	}
}

func getExitStatus() (code int) {
	select {
	case code = <-exitStatus:
	default: // stays zero
	}
	return
}

var client *mqtt.Client

func main() {
	log.SetFlags(0)
	flag.Usage = printManual
	flag.Parse()
	if *quietFlag {
		log.SetOutput(io.Discard)
	}

	clientID, config := parseConfig()
	var err error
	client, err = mqtt.VolatileSession(clientID, config)
	if err != nil {
		log.Fatal(err)
	}

	go applySignals()

	go func() {
		if *publishFlag != "" {
			// publish standard input
			message, err := io.ReadAll(io.LimitReader(os.Stdin, messageMax))
			switch {
			case err != nil:
				log.Fatal(name, ": ", err)
			case len(message) >= messageMax:
				log.Fatalf("%s: standard input reached %d byte limit", name, messageMax)
			}

			ctx, cancel := context.WithTimeout(context.Background(), *timeoutFlag)
			defer cancel()
			err = client.Publish(ctx.Done(), message, *publishFlag)
			switch {
			case err == nil:
				if *verboseFlag {
					log.Printf("%s: published %d bytes to %q", name, len(message), *publishFlag)
				}
			case errors.Is(err, mqtt.ErrClosed), errors.Is(err, mqtt.ErrDown):
				return
			case errors.Is(err, mqtt.ErrCanceled), errors.Is(err, mqtt.ErrAbandoned):
				log.Printf("%s: publish timeout (%s)", name, err)
				setExitStatus(1)
				if err := client.Close(); err != nil {
					log.Print(err)
				}
				return
			default:
				log.Print(err)
				setExitStatus(1)
				_ = client.Close()
				return
			}
		}

		if len(subscribeFlags) != 0 {
			// subscribe & return
			ctx, cancel := context.WithTimeout(context.Background(), *timeoutFlag)
			defer cancel()
			err := client.SubscribeLimitAtMostOnce(ctx.Done(), subscribeFlags...)
			switch {
			case err == nil:
				if *verboseFlag {
					log.Printf("%s: subscribed to %d topic filters", name, len(subscribeFlags))
				}
			case errors.Is(err, mqtt.ErrClosed), errors.Is(err, mqtt.ErrDown):
				return
			case errors.Is(err, mqtt.ErrCanceled), errors.Is(err, mqtt.ErrAbandoned):
				log.Printf("%s: subscribe timeout (%s)", name, err)
				setExitStatus(1)
				if err := client.Close(); err != nil {
					log.Print(err)
				}
				return
			default:
				log.Print(err)
				setExitStatus(1)
				_ = client.Close()
				return
			}
		}

		if *publishFlag == "" && len(subscribeFlags) == 0 {
			// ping exchange
			ctx, cancel := context.WithTimeout(context.Background(), *timeoutFlag)
			defer cancel()
			err := client.Ping(ctx.Done())
			switch {
			case err == nil:
				break // OK
			case errors.Is(err, mqtt.ErrClosed), errors.Is(err, mqtt.ErrDown):
				return
			case errors.Is(err, mqtt.ErrCanceled), errors.Is(err, mqtt.ErrAbandoned):
				log.Printf("%s: ping timeout (%s)", name, err)
				setExitStatus(1)
				if err := client.Close(); err != nil {
					log.Print(err)
				}
				return
			default:
				log.Print(err)
				setExitStatus(1)
				_ = client.Close()
				return
			}
		}

		if len(subscribeFlags) == 0 {
			// graceful shutdown
			ctx, cancel := context.WithTimeout(context.Background(), *timeoutFlag)
			defer cancel()
			err := client.Disconnect(ctx.Done())
			switch {
			case err == nil, errors.Is(err, mqtt.ErrClosed), errors.Is(err, mqtt.ErrDown):
				break
			default:
				log.Print(err)
				setExitStatus(1)
				return
			}
		}
	}()

	defer os.Exit(getExitStatus())

	// read routine
	var big *mqtt.BigMessage
	for {
		message, topic, err := client.ReadSlices()
		switch {
		case err == nil:
			printMessage(message, topic)

		case errors.Is(err, mqtt.ErrClosed):
			return

		case errors.As(err, &big):
			message, err := big.ReadAll()
			if err != nil {
				log.Print(err)
				setExitStatus(1)
				return
			}
			printMessage(message, big.Topic)

		default:
			log.Print(err)

			switch {
			case errors.Is(err, mqtt.ErrProtocolLevel):
				os.Exit(5)
			case errors.Is(err, mqtt.ErrClientID):
				os.Exit(6)
			case errors.Is(err, mqtt.ErrUnavailable):
				os.Exit(7)
			case errors.Is(err, mqtt.ErrAuthBad):
				os.Exit(8)
			case errors.Is(err, mqtt.ErrAuth):
				os.Exit(9)
			}

			setExitStatus(1)
			return
		}
	}
}

func printMessage(message, topic interface{}) {
	switch {
	case *topicFlag && *quoteFlag:
		fmt.Printf("%q%s%q%s", topic, *prefixFlag, message, *suffixFlag)
	case *topicFlag:
		fmt.Printf("%s%s%s%s", topic, *prefixFlag, message, *suffixFlag)
	case *quoteFlag:
		fmt.Printf("%s%q%s", *prefixFlag, message, *suffixFlag)
	default:
		fmt.Printf("%s%s%s", *prefixFlag, message, *suffixFlag)
	}
}

func applySignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	for sig := range signals {
		switch sig {
		case syscall.SIGINT:
			log.Print(name, ": SIGINT received; closing connection…")
			setExitStatus(130)
			switch err := client.Close(); {
			case err == nil, errors.Is(err, mqtt.ErrDown):
				break
			default:
				log.Print(err)
			}

		case syscall.SIGTERM:
			log.Print(name, ": SIGTERM received; sending disconnect…")
			setExitStatus(143)
			ctx, cancel := context.WithTimeout(context.Background(), *timeoutFlag)
			defer cancel()
			switch err := client.Disconnect(ctx.Done()); err != nil {
			case err == nil, errors.Is(err, mqtt.ErrClosed), errors.Is(err, mqtt.ErrDown):
				break
			default:
				log.Print(err)
				setExitStatus(1)
			}
		}
	}
}

func printManual() {
	if *quietFlag {
		return
	}

	log.Print(bold + "NAME\n\t" + name + clear + " \u2014 MQTT broker access\n" +
		"\n" +
		bold + "SYNOPSIS\n" +
		"\t" + bold + name + clear + " [options] address\n" +
		"\n" +
		bold + "DESCRIPTION" + clear + "\n" +
		"\tThe command connects to the address argument, with an option to\n" +
		"\tpublish a message and/or subscribe with topic filters.\n" +
		"\n" +
		"\tWhen the address does not specify a port, then the defaults are\n" +
		"\tapplied, which is 1883 for plain connections and 8883 for TLS.\n" +
		"\n" +
		bold + "OPTIONS" + clear + "\n",
	)

	flag.PrintDefaults()

	log.Print("\n" + bold + "EXIT STATUS" + clear + "\n" +
		"\t(0) no error\n" +
		"\t(1) MQTT operational error\n" +
		"\t(2) illegal command invocation\n" +
		"\t(5) connection refused: unacceptable protocol version\n" +
		"\t(6) connection refused: identifier rejected\n" +
		"\t(7) connection refused: server unavailable\n" +
		"\t(8) connection refused: bad username or password\n" +
		"\t(9) connection refused: not authorized\n" +
		"\t(130) close on SIGINT\n" +
		"\t(143) disconnect on SIGTERM\n" +
		"\n" +

		bold + "EXAMPLES" + clear + "\n" +
		"\tSend a message:\n" +
		"\n" +
		"\t\techo \"hello\" | " + name + " -publish chat/misc localhost\n" +
		"\n" +
		"\tPrint messages:\n" +
		"\n" +
		"\t\t" + name + " -subscribe \"news/#\" -prefix \"📥 \" :1883\n" +
		"\n" +
		"\tHealth check:\n" +
		"\n" +
		"\t\t" + name + " -tls q1.example.com:8883 || echo \"exit $?\"\n" +
		"\n" +

		bold + "BUGS" + clear + "\n" +
		"\tReport bugs at <https://github.com/pascaldekloe/mqtt/issues>.\n" +
		"\n" +

		"SEE ALSO" + clear + "\n\tmosquitto_pub(1)\n",
	)
}
