package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisClient struct {
	client    *redis.Client
	priority  int
	isHealthy atomic.Bool
	name      string
}

type RedisWrapper struct {
	clients      []*RedisClient
	activeClient *RedisClient
	mu           sync.RWMutex
	forceInclude bool

	healthCheckInterval time.Duration
	stopHealthCheck     chan struct{}
}

func NewRedisWrapper(forceInclude bool, healthCheckInterval time.Duration) *RedisWrapper {
	return &RedisWrapper{
		forceInclude:        forceInclude,
		healthCheckInterval: healthCheckInterval,
	}
}

func (rw *RedisWrapper) AddClient(ctx context.Context, client *redis.Client, priority int, name string) {
	isHealthy := client.Ping(ctx).Err() == nil

	if !isHealthy && !rw.forceInclude {
		return
	}

	newClient := &RedisClient{
		client:   client,
		priority: priority,
		name:     name,
	}
	newClient.isHealthy.Store(isHealthy)

	rw.mu.Lock()
	rw.clients = append(rw.clients, newClient)
	rw.mu.Unlock()
}

func (rw *RedisWrapper) getActiveClient() *RedisClient {
	rw.mu.RLock()
	defer rw.mu.RUnlock()
	return rw.activeClient
}

func (rw *RedisWrapper) updateActiveClient() {
	rw.mu.Lock()
	defer rw.mu.Unlock()

	var best *RedisClient
	for _, c := range rw.clients {
		if !c.isHealthy.Load() {
			continue
		}
		if best == nil || c.priority < best.priority {
			best = c
		}
	}
	rw.activeClient = best
}

func (rw *RedisWrapper) StartHealthCheck(ctx context.Context) {
	rw.checkClientsHealth(ctx)

	// this flagger allows start and stop healthchecks
	rw.stopHealthCheck = make(chan struct{})
	ticker := time.NewTicker(rw.healthCheckInterval)

	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-rw.stopHealthCheck:
				return
			case <-ticker.C:
				rw.checkClientsHealth(ctx)
			}
		}
	}()
}

func (rw *RedisWrapper) StopHealthCheck() {
	close(rw.stopHealthCheck)
}

func (rw *RedisWrapper) checkClientsHealth(ctx context.Context) {
	rw.mu.RLock()
	clients := rw.clients
	rw.mu.RUnlock()

	var wg sync.WaitGroup
	for _, c := range clients {
		wg.Go(func() {
			isHealthy := c.client.Ping(ctx).Err() == nil
			c.isHealthy.Store(isHealthy)
		})
	}
	wg.Wait()

	rw.updateActiveClient()
}

func (rw *RedisWrapper) Execute(ctx context.Context, fn func(*redis.Client) error) error {
	active := rw.getActiveClient()
	if active == nil {
		return errors.New("no healthy client available")
	}

	err := fn(active.client)
	if err != nil {
		active.isHealthy.Store(false)
		rw.updateActiveClient()

		active = rw.getActiveClient()
		if active == nil {
			return errors.New("no healthy client available after failover")
		}
		err = fn(active.client)
	}

	fmt.Printf("used client: %s\n", active.name)
	return err
}
