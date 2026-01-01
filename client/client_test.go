package client

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestClientPing(t *testing.T) {
	client, err := NewClient("localhost:5001")
	if err != nil {
		return
	}
	defer client.Close()

	val, err := client.Ping(context.Background())
	if err != nil {
		t.Fatalf("ERROR OCCURED WHILE PING %s", err)
	}

	fmt.Println("Received Message: ", val)
}

func TestNewClient1(t *testing.T) {
	client, err := NewClient("localhost:5001")
	if err != nil {
		return
	}
	defer client.Close()

	if err := client.Set(context.TODO(), "foo", 89); err != nil {
		log.Fatal(err)
	}
	val, err := client.Get(context.TODO(), "foo")
	if err != nil {
		log.Fatal(err)
	}
	n, _ := strconv.Atoi(val)

	fmt.Println(n)

	fmt.Printf("GET of Type %T =>", val)
}

func TestNewCients(t *testing.T) {
	nClients := 10
	wg := sync.WaitGroup{}
	wg.Add(nClients)
	for i := 0; i < nClients; i++ {
		go func(i int) {
			client, err := NewClient("localhost:5001")
			if err != nil {
				return
			}
			defer client.Close()

			key := fmt.Sprintf("foo_client_%d", i)
			value := fmt.Sprintf("client_bar_%d", i)
			time.Sleep(time.Second)
			if err := client.Set(context.TODO(), key, value); err != nil {
				log.Fatal(err)
			}
			val, err := client.Get(context.TODO(), key)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("\nclient %s got this val back => %s", key, val)
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func TestClientExists(t *testing.T) {
	client, err := NewClient("localhost:5001")
	if err != nil {
		return
	}
	defer client.Close()

	if err := client.Set(context.Background(), "foo", "bar"); err != nil {
		log.Fatal(err)
	}

	time.Sleep(time.Second * 2)

	val, err := client.Exists(context.Background(), "foo")
	if err != nil {
		t.Fatalf("Error in exists: %d", err)
	}
	fmt.Println("Response for exists: ", val)

	val1, err := client.Exists(context.Background(), "doo")
	if err != nil {
		t.Fatalf("Error in exists: %d", err)
	}
	fmt.Println("Response for exists: ", val1)
}

func TestClientFlushDB(t *testing.T) {
	client, err := NewClient("localhost:5001")
	if err != nil {
		return
	}
	defer client.Close()

	if err := client.Set(context.Background(), "foo", "bar"); err != nil {
		log.Fatal(err)
	}
	if err := client.Set(context.Background(), "doo", "bar"); err != nil {
		log.Fatal(err)
	}

	time.Sleep(time.Second * 2)

	val, err := client.Exists(context.Background(), "foo")
	if err != nil {
		t.Fatalf("Error in exists: %d", err)
	}
	fmt.Println("Response for exists: ", val)

	val, err = client.Exists(context.Background(), "doo")
	if err != nil {
		t.Fatalf("Error in exists: %d", err)
	}
	fmt.Println("Response for exists: ", val)

	time.Sleep(time.Second * 2)
	if err := client.FlushDB(context.Background()); err != nil {
		t.Fatalf("Error while flushing %d", err)
	}

	time.Sleep(time.Second * 2)
	val1, err := client.Exists(context.Background(), "foo")
	if err != nil {
		t.Fatalf("Error in exists: %d", err)
	}
	fmt.Println("Response for exists: ", val1)
	val1, err = client.Exists(context.Background(), "doo")
	if err != nil {
		t.Fatalf("Error in exists: %d", err)
	}
	fmt.Println("Response for exists: ", val1)
}

// Client Subscribe command test
func TestClientSubscribe(t *testing.T) {
	ctx := context.Background()
	c, _ := NewClient("localhost:5001")

	// Subscribe
	ps, err := c.Subscribe(ctx, "chan")
	if err != nil {
		log.Fatal(err)
	}

	// Listen loop
	go func() {

		for {
			msg, err := ps.ReceiveMessage(ctx)
			if err != nil {
				log.Fatal("receive failed:", err)
			}
			fmt.Printf("channel=%s payload=%s\n", msg.Channel, msg.Payload)
		}
	}()

	time.Sleep(time.Second * 10)

	ps.Unsubscribe(context.Background(), "chan")
}

func TestClientDelete(t *testing.T) {
	ctx := context.Background()
	c, _ := NewClient("localhost:5001")

	if err := c.Set(ctx, "foo", "bar"); err != nil {
		fmt.Errorf("Error while Set operation %d", err)
	}

	time.Sleep(time.Second * 2)

	res, err := c.Exists(ctx, "foo")

	if err != nil {
		fmt.Println("Error while exists check")
	}

	fmt.Println("Exists response: ", res)

	if err := c.Delete(ctx, "foo"); err != nil {
		fmt.Println("Error while Deleting foo")
	}

	time.Sleep(time.Second * 2)

	res2, err2 := c.Exists(ctx, "foo")

	if err2 != nil {
		fmt.Println("Error while exists check")
	}
	fmt.Println("Exists response: ", res2)
}

func TestClientPublish(t *testing.T) {
	ctx := context.Background()
	c, _ := NewClient("localhost:5001")
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		for i := 0; true; i++ {
			time.Sleep(time.Second * 1)
			msg := fmt.Sprintf("%d th message", i)
			c.Publish(ctx, "chan", msg)
		}
	}()
	wg.Wait()
}

func TestClientEcho(t *testing.T) {
	ctx := context.Background()
	client, _ := NewClient("localhost:5001")
	echoMsg := "HELLO"
	msg, err := client.Echo(ctx, echoMsg)

	if err != nil {
		fmt.Errorf("ERROR OCCURED IN ECHO: %d", err)
	}

	fmt.Println("Message Received: ", msg)

}

func TestMaxConn(t *testing.T) {
	for i := 0; i < 10; i++ {
		go func() {
			cl, err := NewClient("localhost:5001")
			if err != nil {
				fmt.Println("Error while connecting", err)
			}
			fmt.Println("Client connected", cl)
		}()
	}
	time.Sleep(time.Second * 5)
}

func TestTimeout(t *testing.T) {
	client, err := NewClient("localhost:5001")
	if err != nil {
		fmt.Errorf("Client connection error: %d", err)
	}
	client.Set(context.Background(), "foo", "bar")
	time.Sleep(time.Second * 3)
}
