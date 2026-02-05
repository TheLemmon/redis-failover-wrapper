package main

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

func main() {
	rc1 := redis.NewClient(&redis.Options{
		Addr:     "localhost:12000",
		Password: "",
		DB:       0,
		Protocol: 2,
	})

	rc2 := redis.NewClient(&redis.Options{
		Addr:     "localhost:12001",
		Password: "",
		DB:       0,
		Protocol: 2,
	})

	// create a new wrapper that include no healthy servers
	// and do healthcheck every 500 ms
	wrapper := NewRedisWrapper(true, 500*time.Millisecond)
	wrapper.AddClient(ctx, rc1, 0, "cluster primary", &ClusterConfig{
		Address:  "https://localhost:9443/v1/bdbs/2",
		User:     "admin@admin.com",
		Password: "temporal123",
	})
	wrapper.AddClient(ctx, rc2, 1, "cluster backup", &ClusterConfig{
		Address:  "https://localhost:9444/v1/bdbs/2",
		User:     "admin@admin.com",
		Password: "temporal123",
	})

	// this action is a blocker, because it waits to ping all the clients
	wrapper.StartHealthCheck(ctx)

	startTest(wrapper)
}

type Result struct {
	InsertionTime int
	Status        bool
}

func startTest(wrapper *RedisWrapper) {
	ticker := time.NewTicker(100 * time.Millisecond)
	done := make(chan bool)

	go func() {
		count := 0
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				count++
				key := fmt.Sprintf("key-%d", count)
				err := wrapper.ExecuteWithFallback(ctx, func(c *redis.Client) error {
					return c.Set(ctx, key, "value", 5*time.Second).Err()
				})
				now := time.Now().Format("15:04:05")
				if err != nil {
					fmt.Printf("[%s] #%d FAIL: %v\n", now, count, err)
				} else {
					fmt.Printf("[%s] #%d OK\n", now, count)
				}
			}
		}
	}()

	time.Sleep(2 * time.Minute)
	ticker.Stop()
	done <- true
	fmt.Println("test finished")
}
