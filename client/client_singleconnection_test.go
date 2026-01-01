package client

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestFullClient(t *testing.T) {
	client, err := NewClient("localhost:5001")

	if err != nil {
		fmt.Errorf("Client Connection Error: %d", err)
	}

	ctx := context.Background()

	if err := client.Set(ctx, "foo", "bar"); err != nil {
		fmt.Errorf("Set command error: %d", err)	
	}
	
	time.Sleep(time.Second* 5)
	
	if err := client.Set(ctx, "foo", "dar"); err != nil {
		fmt.Errorf("Set command error: %d", err)	
	}
 
	res, err := client.Get(ctx, "foo")

	if err != nil {
		fmt.Errorf("Error in get command: %d",err)
	}

	fmt.Println("The response for get command is : ", res)

	if err := client.FlushDB(ctx); err != nil {
		fmt.Errorf("Error while flushing the DB: %d", err)
	}

	res, err = client.Get(ctx, "foo")

	if err != nil {
		fmt.Errorf("Error in get command: %d",err)
	}

	if res == "" {
		fmt.Println("No value found for res")
	}
}
