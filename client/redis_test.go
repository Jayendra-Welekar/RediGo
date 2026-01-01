package client

import (
	"context"
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestPing(t *testing.T) {
	c2 := redis.NewClient(&redis.Options{
		Addr:     "localhost:5001",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	defer c2.Close()

	pong, err := c2.Ping(context.Background()).Result()
	if err != nil {
		t.Fatalf("Ping failed: %v", err)
	}
	if pong != "PONG" {
		t.Errorf("Expected PONG, got %s", pong)
	}
	fmt.Println("Go result : ", pong)
}

func TestEcho(t *testing.T) {
	rc := redis.NewClient(&redis.Options{
		Addr:     "localhost:5001",
		Password: "",
		DB:       0,
	})

	defer rc.Close()

	echotext, err := rc.Echo(context.Background(), "ECHO TEXT").Result()
	if err != nil {
		t.Fatalf("Expected exchotextGET of Type string =>PASS not received %s ", err)
	}
	fmt.Println("Echo text that is received: ", echotext)
}

func TestGet(t *testing.T) {
	c1 := redis.NewClient(&redis.Options{
		Addr:     "localhost:5001",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	val, err := c1.Get(context.Background(), "foo").Result()
	if err != nil {
		fmt.Println("Error Occured", err)
	}
	fmt.Println("Value of the get command is: ", val)
}

func TestWrite(t *testing.T) {
	wg := sync.WaitGroup{}
	wg.Add(3)
	go func() {
		defer wg.Done()
		c1 := redis.NewClient(&redis.Options{
			Addr:     "localhost:5001",
			Password: "", // no password set
			DB:       0,  // use default DB
		})
		err := c1.Publish(context.Background(), "dummychannel", "Hello Redis!").Err()
		if err != nil {
			log.Fatalf("Failed to publish message: %v", err)
		}
	}()
	wg.Wait()
}

func TestMulti(t *testing.T) {
	wg := sync.WaitGroup{}
	wg.Add(3)
	go func() {
		defer wg.Done()
		c1 := redis.NewClient(&redis.Options{
			Addr:     "localhost:5001",
			Password: "", // no password set
			DB:       0,  // use default DB
		})
		err1 := c1.Set(context.Background(), "foo", "bar", 0).Err()

		if err1 != nil {
			panic(err1)
		}
	}()
	time.Sleep(time.Second)
	go func() {
		defer wg.Done()
		c2 := redis.NewClient(&redis.Options{
			Addr:     "localhost:5001",
			Password: "", // no password set
			DB:       0,  // use default DB
		})

		time.Sleep(time.Second * 2)

		val, err2 := c2.Get(context.Background(), "foo").Result()

		if err2 != nil {
			panic(err2)
		}

		fmt.Println("The value for key is ", val)
	}()

	go func() {
		defer wg.Done()
		c2 := redis.NewClient(&redis.Options{
			Addr:     "localhost:5001",
			Password: "", // no password set
			DB:       0,  // use default DB
		})

		time.Sleep(time.Second * 2)

		pubsub := c2.Subscribe(context.Background(), "chanName")

		time.Sleep(time.Second)

		pubsub.Close()
	}()

	wg.Wait()
}

func TestNewRedisClient(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:5001",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	err := rdb.Set(context.Background(), "foo", "bar", 0).Err()
	if err != nil {
		panic(err)
	}
	time.Sleep(time.Second * 2)
	err = rdb.Get(context.Background(), "foo").Err()
	if err != nil {
		panic(err)
	}
}

func TestFlushDB(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:5001",
		Password: "",
		DB:       0,
	})
	defer rdb.Close()

	// Set key first
	err := rdb.Set(context.Background(), "foo", "bar", 0).Err()
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	time.Sleep(time.Second)
	// Flush DB
	err = rdb.FlushDB(context.Background()).Err()
	if err != nil {
		t.Fatalf("FlushDB failed: %v", err)
	}

	time.Sleep(time.Second)
	// Verify key is gone
	val, err := rdb.Get(context.Background(), "foo").Result()
	if err == nil {
		t.Fatalf("Expected key to be deleted after FlushDB, got: %s", val)
	}
}

func TestExist(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:5001",
		Password: "",
		DB:       0,
	})

	err := rdb.Set(context.Background(), "foo", "bar", 0).Err()
	if err != nil {
		panic(err)
	}
	time.Sleep(time.Second * 2)
	res := rdb.Exists(context.Background(), "foo")
	fmt.Println("Response: ", res.Val())
	res2 := rdb.Exists(context.Background(), "foo2")
	fmt.Println("Response for foo2: ", res2.Val())
}

func TestPublish(t *testing.T) {
	wg := sync.WaitGroup{}

	// Subscriber goroutine - listens for messages
	wg.Add(1)
	c2 := redis.NewClient(&redis.Options{
		Addr:     "localhost:5001",
		Password: "",
		DB:       0,
	})
	go func() {
		for i := 0; true; i++ {
			time.Sleep(time.Second * 1)
			msg := fmt.Sprintf("%d th message", i)
			c2.Publish(context.Background(), "chan", msg)
		}
	}()
	wg.Wait()
}

func TestSubscribe1(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c2 := redis.NewClient(&redis.Options{
		Addr:     "localhost:5001",
		Password: "",
		DB:       0,
	})
	defer c2.Close()

	pubsub := c2.Subscribe(ctx, "chan")
	defer pubsub.Close()

	// 1) Receive the SUBSCRIBE confirmation
	msg, err := pubsub.ReceiveMessage(ctx)
	if err != nil {
		t.Fatalf("failed to receive subscribe confirmation: %v", err)
	}
	t.Logf("Subscribe confirm: channel=%s payload=%s", msg.Channel, msg.Payload)
	// 2) Start listener goroutine
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				msg, err := pubsub.ReceiveMessage(ctx)
				if err != nil {
					// This will trigger when we unsubscribe/close
					t.Logf("ReceiveMessage error: %v", err)
					return
				}
				t.Logf("Received message: ch=%s payload=%s", msg.Channel, msg.Payload)
			}
		}
	}()

	// 3) Wait some time to allow manual PUBLISH from redis-cli or your tests
	time.Sleep(5 * time.Second)

	// 4) Unsubscribe and cancel
	if err := pubsub.Unsubscribe(ctx, "chan"); err != nil {
		t.Fatalf("unsubscribe failed: %v", err)
	}

	// Optionally read the unsubscribe confirmation
	msg, err = pubsub.ReceiveMessage(ctx)
	if err != nil {
		t.Fatalf("failed to receive unsubscribe confirmation: %v", err)
	}
	t.Logf("Unsubscribe confirm: %v", msg)

	cancel()
	wg.Wait()
}

func TestSubscribe(t *testing.T) {
	wg := sync.WaitGroup{}

	// Subscriber goroutine - listens for messages
	wg.Add(1)
	c2 := redis.NewClient(&redis.Options{
		Addr:     "localhost:5001",
		Password: "",
		DB:       0,
	})
	pubsub := c2.Subscribe(context.Background(), "chan")
	msg, err := pubsub.ReceiveMessage(context.Background())
	if err != nil {
		fmt.Errorf("error occured: %d", err)
	}

	fmt.Println("Confirmation message received: ", msg)

	go func() {
		for {
			select {
			case <-context.Background().Done():
				fmt.Println("Done")
				return
			default:
				msg, err := pubsub.ReceiveMessage(context.Background())
				if err != nil {
					t.Errorf("Following error occured %v", err)
					return
				}
				fmt.Println("Received  message: ", msg)
			}
		}
	}()
	time.Sleep(time.Second * 20)
	pubsub.Unsubscribe(context.Background(), "chan")
	wg.Wait()
}

func TestDelete(t *testing.T) {
	wg := sync.WaitGroup{}

	// Subscriber goroutine - listens for messages
	wg.Add(1)
	c2 := redis.NewClient(&redis.Options{
		Addr:     "localhost:5001",
		Password: "",
		DB:       0,
	})

	if err := c2.Set(context.Background(), "foo", "bar", 0); err != nil {
		fmt.Errorf("Error while setting %s", err)
	}

	time.Sleep(time.Second * 1)

	res := c2.Exists(context.Background(), "foo")

	fmt.Println("Response for Exists is", res)

	time.Sleep(time.Second * 1)

	err := c2.Del(context.Background(), "foo")

	if err != nil {

		fmt.Errorf("Error while checking exitss %d", err)
	}

	res = c2.Exists(context.Background(), "foo")

	fmt.Println("Response for Exists is", res)

}
