package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"redisGo/client"
)

func TestFooBar(t *testing.T) {
	in := map[string]string{
		"first": "1",
		"second": "2",
	}	

	out := respWriteMap(in)
	fmt.Println(out)
}

func TestServerWithMultiClients(t *testing.T) {
	server := NewServer(Config{})
	go func() {
		log.Fatal(server.Start())
	}()
	time.Sleep(time.Second)
	nClients := 10
	wg := sync.WaitGroup{}
	wg.Add(nClients)
	for i := 0; i < nClients; i++ {
		go func(i int) {
			client, err := client.NewClient("localhost:5001")
			if err != nil {
				return
			}
			defer wg.Done()
			defer client.Close()

			key := fmt.Sprintf("foo_client_%d", i)
			value := fmt.Sprintf("client_bar_%d", i)
			time.Sleep(time.Second)
			if err := client.Set(context.TODO(), key, value); err != nil {
				log.Fatal(err)
			}
			time.Sleep(time.Second)
			val, err := client.Get(context.TODO(), key)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("\nclient %s got this val back => %s", key, val)
		}(i)
	}
	wg.Wait()
	time.Sleep(time.Second * 2)
	
	if len(server.peers) != 0 {
		t.Fatalf("expected 0 peers but go %d", len(server.peers))
	}
}
