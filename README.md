[![Northvolt](https://github.com/pascaldekloe/mqtt/raw/master/doc/northvolt.svg)](https://northvolt.com)

[![API Documentation](https://godoc.org/github.com/pascaldekloe/mqtt?status.svg)](https://godoc.org/github.com/pascaldekloe/mqtt)
[![Build Status](https://travis-ci.org/pascaldekloe/mqtt.svg?branch=master)](https://travis-ci.org/pascaldekloe/mqtt)

# About

… an MQTT client library for the Go programming language. Great care was taken
to provide correctness in all scenario, including the error states. Message
transfers in both directions have zero-copy. All errors are propagated through
the API—no internal logging.

The development was kindly sponsored by [Northvolt](https://northvolt.com), as a
gift to the open-source community.

This is free and unencumbered software released into the
[public domain](https://creativecommons.org/publicdomain/zero/1.0).


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
Configure the client with `AtLeastOnceMax` and `ExactlyOnceMax` set to
either `0` or `1` to prevent seemingly random connection errors.
