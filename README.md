# MQTT🤖

## About

… an MQTT client library for the Go programming language. Great care was taken
to provide correctness in all scenario, including the error states. Message
transfers in both directions have zero-copy. Errors are propagated through the
API. There is no internal logging by design.

The development was kindly sponsored by [Northvolt](https://northvolt.com), as a
gift to the open-source community.

This is free and unencumbered software released into the
[public domain](https://creativecommons.org/publicdomain/zero/1.0).

[![Go Reference](https://pkg.go.dev/badge/github.com/pascaldekloe/mqtt.svg)](https://pkg.go.dev/github.com/pascaldekloe/mqtt@v1.0.0-rc)
[![Build Status](https://travis-ci.org/pascaldekloe/mqtt.svg?branch=master)](https://travis-ci.org/pascaldekloe/mqtt)


## Introduction

Message transfer without confirmation can be quite simple.

```go
	err := client.Publish(nil, []byte("20.8℃"), "bedroom")
	if err != nil {
		log.Print("thermostat update lost: ", err)
		return
	}
```

A read routine sees inbound messages one by one.

```go
	for {
		message, topic, err := client.ReadSlices()
		switch {
		case err == nil:
			r, _ := utf8.DecodeLastRune(message)
			switch r {
			case 'K', '℃', '℉':
				log.Printf("%s at %q", message, topic)
			}

		case errors.Is(err, mqtt.ErrClosed):
			return // terminated

		default:
			log.Print("broker unavailable: ", err)
			time.Sleep(time.Second) // backoff
		}
	}
```

The [examples](https://pkg.go.dev/github.com/pascaldekloe/mqtt@v1.0.0-rc#pkg-examples)
from the package documentation provide a good start with detailed configuration
options.


## Command-Line Client

Run `go install github.com/pascaldekloe/mqtt/cmd/mqttc` to build the binary.

```
NAME
	mqttc — MQTT broker access

SYNOPSIS
	mqttc [options] address

DESCRIPTION
	The command connects to the address argument, with an option to
	publish a message and/or subscribe with topic filters.

	When the address does not specify a port, then the defaults are
	applied, which is 1883 for plain connections and 8883 for TLS.

OPTIONS
  -client identifier
    	Use a specific client identifier. (default "generated")
  -net name
    	Select the network by name. Valid alternatives include tcp4,
    	tcp6 and unix. (default "tcp")
  -pass file
    	The file content is used as a password.
  -prefix string
    	Print a string before each inbound message.
  -publish topic
    	Send a message to a topic. The payload is read from standard
    	input.
  -quiet
    	Suppress all output to standard error. Error reporting is
    	deduced to the exit code only.
  -quote
    	Print inbound topics and messages as quoted strings.
  -server name
    	Use a specific server name with TLS
  -subscribe filter
    	Listen with a topic filter. Inbound messages are printed to
    	standard output until interrupted by a signal(3). Multiple
    	-subscribe options may be applied together.
  -suffix string
    	Print a string after each inbound message. (default "\n")
  -timeout duration
    	Network operation expiry. (default 4s)
  -tls
    	Secure the connection with TLS.
  -topic
    	Print the respective topic of each inbound message.
  -user name
    	The user name may be used by the broker for authentication
    	and/or authorization purposes.
  -verbose
    	Produces more output to standard error for debug purposes.

EXIT STATUS
	(0) no error
	(1) MQTT operational error
	(2) illegal command invocation
	(5) connection refused: unacceptable protocol version
	(6) connection refused: identifier rejected
	(7) connection refused: server unavailable
	(8) connection refused: bad username or password
	(9) connection refused: not authorized
	(130) close on SIGINT
	(143) disconnect on SIGTERM

EXAMPLES
	Send a message:

		echo "hello" | mqttc -publish chat/misc localhost

	Print messages:

		mqttc -subscribe "news/#" -prefix "📥 " :1883

	Health check:

		mqttc -tls q1.example.com:8883 || echo "exit $?"

BUGS
	Report bugs at <https://github.com/pascaldekloe/mqtt/issues>.

SEE ALSO
	mosquitto_pub(1)
```


## Standard Compliance

The implementation follows version 3.1.1 of the
[OASIS specification](http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html)
in a strict manner. Support for the originating
[IBM specification](https://public.dhe.ibm.com/software/dw/webservices/ws-mqtt/mqtt-v3r1.html)
may be added at some point in time.

There are no plans to support protocol version 5. Version 3 is lean and well
suited for IOT. The additions in version 5 may be more of a fit for backend
computing.

AWS IoT [Amazon] deviates from the standard on several significant
[points](https://docs.aws.amazon.com/iot/latest/developerguide/mqtt.html#mqtt-differences).
Configure the client with `AtLeastOnceMax` and `ExactlyOnceMax` each set to
either `0` or `1`, to prevent seemingly random connection errors.
