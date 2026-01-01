package client

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"strings"

	"github.com/tidwall/resp"
)

type Client struct {
	addr string
	conn net.Conn
}

type PubSub struct {
	client  *Client
	channel string
}

type Message struct {
	Channel string
	Payload string
}

func NewClient(addr string) (*Client, error) {
	if addr == "" {
		return nil, fmt.Errorf("invalid address")
	}

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial failed: %w", err)
	}

	return &Client{
		addr: addr,
		conn: conn,
	}, nil
}

func (c *Client) readRESP() (resp.Value, error) {
	// Read full RESP response
	rd := resp.NewReader(c.conn)
	val, _, err := rd.ReadValue()
	if err != nil {
		return resp.Value{}, fmt.Errorf("read RESP failed: %w", err)
	}

	return val, nil
}

func (c *Client) Get(ctx context.Context, key string) (string, error) {
    val, err := c.Exec(ctx, "GET", key)
    if err != nil {
        return "", err
    }
    
    // Check for nil bulk string: $-1\r\n
    if val.Type() == resp.BulkString && val.IsNull() {
        return "", nil  
		}
    
    if val.Type() == resp.BulkString {
        return val.String(), nil
    }
    
    return "", fmt.Errorf("unexpected type: %v", val.Type())
}


func (c *Client) Set(ctx context.Context, key string, val interface{}) error {
	fmt.Println("given the set command", key, val)

	// Fix: val should be string, not int
	strVal := fmt.Sprintf("%v", val)

	buf := &bytes.Buffer{}
	wr := resp.NewWriter(buf)
	wr.WriteArray([]resp.Value{
		resp.StringValue("set"),
		resp.StringValue(key),
		resp.StringValue(strVal), // Fixed: resp.String, not IntegerValue
	})

	_, err := c.conn.Write(buf.Bytes())
	if err != nil {
		return fmt.Errorf("write failed: %w", err)
	}

	// Read OK response
	valResp, err := c.readRESP()
	if err != nil {
		return err
	}

	if valResp.String() != "OK" {
		return fmt.Errorf("expected OK, got %v", valResp)
	}

	return nil
}

func (c *Client) Ping(ctx context.Context) (string, error) {
	buf := &bytes.Buffer{}
	wr := resp.NewWriter(buf)
	wr.WriteArray([]resp.Value{resp.StringValue("ping")})

	_, err := c.conn.Write(buf.Bytes())
	if err != nil {
		return "", err
	}

	respVal, err := c.readRESP()
	if err != nil {
		return "", err
	}
	return respVal.String(), nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) FlushDB(ctx context.Context, keys ...interface{}) error {
	_, err := c.Exec(ctx, "FLUSHDB", keys...)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) Exists(ctx context.Context, keys ...interface{}) (int, error) {
	val, err := c.Exec(ctx, "EXISTS", keys...)
	if err != nil {
		return -1, err
	}
	return int(val.Integer()), nil
}

func (c *Client) Exec(ctx context.Context, cmd string, args ...interface{}) (resp.Value, error) {
	// Convert args to strings
	strArgs := make([]string, len(args))
	for i, arg := range args {
		strArgs[i] = fmt.Sprintf("%v", arg)
	}

	buf := &bytes.Buffer{}
	wr := resp.NewWriter(buf)
	values := make([]resp.Value, len(strArgs)+1)
	values[0] = resp.StringValue(strings.ToLower(cmd))
	for i, arg := range strArgs {
		values[i+1] = resp.StringValue(arg)
	}
	wr.WriteArray(values)

	_, err := c.conn.Write(buf.Bytes())
	if err != nil {
		return resp.Value{}, fmt.Errorf("write failed: %w", err)
	}

	return c.readRESP()
}

func (c *Client) writeArray(cmd string, args ...string) error {
    buf := &bytes.Buffer{}
    wr := resp.NewWriter(buf)

    values := make([]resp.Value, len(args)+1)
    values[0] = resp.StringValue(strings.ToLower(cmd))
    for i, arg := range args {
        values[i+1] = resp.StringValue(arg)
    }
    wr.WriteArray(values)

    _, err := c.conn.Write(buf.Bytes())
    if err != nil {
        return fmt.Errorf("write failed: %w", err)
    }
    return nil
}


func (c *Client) Subscribe(ctx context.Context, channel string) (*PubSub, error) {
	 err := c.writeArray("SUBSCRIBE", channel)
	if err != nil {
		return nil,err
	}
	return &PubSub{
		client:  c,
		channel: channel,
	},nil
}

func (ps *PubSub) ReceiveMessage(ctx context.Context) (*Message, error) {
	val, err := ps.client.readRESP()
	if err != nil {
		return nil, err
	}
	if val.Type() != resp.Array {
		return nil, fmt.Errorf("Unexpected reply type from server: %v ", val.Type())
	}

	arr := val.Array()
	if len(arr) < 3 {
		return nil, fmt.Errorf("invalid pubsub message: %#v", arr)
	}
	kind := arr[0].String()
	switch kind {
	case "message":
		return &Message{
			Channel: arr[1].String(),
			Payload: arr[2].String(),
		}, nil
	case "subscribe", "unsubscribe":
		return &Message{
			Channel: arr[1].String(),
			Payload: kind, // or empty
		}, nil
	default:
		return nil, fmt.Errorf("unknown pubsub kind: %s", kind)
	}
}

func (ps *PubSub) Unsubscribe(ctx context.Context, channels ...string) error {
	for _, channel := range channels {
		val, err := ps.client.Exec(ctx, "UNSUBSCRIBE", channel)
		if err != nil {
			return fmt.Errorf("Unsubscribe to channel %s failed: %w", channel, err)
		}
		arr := val.Array()
		if val.Type() == resp.Array {
			if len(arr) >= 3 && arr[0].String() == "unsubscribe" {
				fmt.Printf("Unsubscribed from %s, remaining: %s\n", arr[1], arr[2])
			}
		}
	}
	return nil
}

func (c *Client) Delete(ctx context.Context, key string) error {
	_, err := c.Exec(ctx, "DEL", key)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) Publish(ctx context.Context, channel string, val string) error {
	_, err := c.Exec(ctx, "PUBLISH", channel, val)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) Echo(ctx context.Context, val string) (string, error) {
	msg, err := c.Exec(ctx, "ECHO", val)
	if err != nil {
		return "",err
	}
	return msg.String(), nil
}

func (c *Client) Quit(ctx context.Context) error {
    _, err := c.Exec(ctx, "QUIT") 
		c.Close()                    
    return err
}

