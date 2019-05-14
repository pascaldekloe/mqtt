package mqtt

import (
	"context"
	"net"
)

// Receive is invoked for inbound messages with AtMostOnce or AtLeastOnce QoS.
// AtLeastOnce causes retries on error returns.
type Receive func(ctx context.Context, topic string, message []byte) error

// Client is the initiator of the protocol.
type Client struct {
	ctx         context.Context
	conn        net.Conn
	listener    Receive
	writePacket packet
}

// Connect initiates the protocol over a transport layer such as *net.TCP or
// *tls.Conn. The session context is applied to all the client operations too.
func Connect(ctx context.Context, conn net.Conn, attrs *Attributes, listener Receive) (*Client, error) {
	var writePacket packet
	writePacket.connReq(attrs)

	_, err := conn.Write(writePacket.buf)
	if err != nil {
		return nil, err
	}

	return &Client{ctx, conn, listener, writePacket}, nil
}

// Publish submits a message on a topic with a specific quality of service.
// AtMostOnce returns after network submission, AtLeastOnce awaits receival
// acknowledgement and AtMostOnce undergoes a two-stage acknowledegement.
func (c *Client) Publish(ctx context.Context, topic string, message []byte, deliver QoS) error {
	panic("TODO")
}

// PublishRetained acts like Publish, but causes the message to be stored on the
// server, so that it can be delivered to future subscribers.
func (c *Client) PublishRetained(ctx context.Context, topic string, message []byte, deliver QoS) error {
	panic("TODO")
}

// Subscribe requests a subscription for all topics that match the filter.
// The requested quality of service is a maximum for the server.
func (c *Client) Subscribe(ctx context.Context, topicFilter string, max QoS) error {
	panic("TODO")
}

// Unsubscribe requests a Subscribe cancelation.
func (c *Client) Unsubscribe(ctx context.Context, topicFilter string) error {
	panic("TODO")
}

// Disconnect is a graceful termination, which also discards the Will.
// The connection is closed uppon succes.
func (c *Client) Disconnect() error {
	panic("TODO")
}
