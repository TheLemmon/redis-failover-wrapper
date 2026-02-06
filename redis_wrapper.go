package main

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

type BDBResponse struct {
	UID         int          `json:"uid"`
	Name        string       `json:"name"`
	Status      string       `json:"status"`
	CRDTSync    string       `json:"crdt_sync"`
	CRDTSources []CRDTSource `json:"crdt_sources"`
}

type CRDTSource struct {
	UID        int    `json:"uid"`
	Status     string `json:"status"`
	Lag        int    `json:"lag"`
	LastUpdate string `json:"last_update"`
}

type ClusterConfig struct {
	User     string
	Password string
	Address  string
}

type RedisClient struct {
	client        *redis.Client
	priority      int
	isHealthy     atomic.Bool
	isSync        atomic.Bool
	clusterConfig *ClusterConfig
	name          string
}

type RedisWrapper struct {
	clients      []*RedisClient
	activeClient *RedisClient
	mu           sync.RWMutex
	httpClient   *http.Client

	healthCheckInterval time.Duration
	stopHealthCheck     chan struct{}
}

func NewRedisWrapper(forceInclude bool, healthCheckInterval time.Duration) *RedisWrapper {
	return &RedisWrapper{
		healthCheckInterval: healthCheckInterval,
		httpClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
			Timeout: 5 * time.Second,
		},
	}
}

func (rw *RedisWrapper) checkClusterAvailability(ctx context.Context, client *redis.Client, clusterConfig *ClusterConfig) (isHealthy bool, isSync bool) {
	var wg sync.WaitGroup

	// Ejecutar PING y HTTP request en paralelo
	wg.Go(func() {
		val, err := client.Ping(ctx).Result()
		isHealthy = (val == "PONG" && err == nil)
	})

	wg.Go(func() {
		req, err := http.NewRequest("GET", clusterConfig.Address, nil)
		if err != nil {
			isSync = false
			return
		}

		creds := fmt.Sprintf("%s:%s", clusterConfig.User, clusterConfig.Password)
		auth := base64.StdEncoding.EncodeToString([]byte(creds))
		req.Header.Set("Authorization", "Basic "+auth)

		resp, err := rw.httpClient.Do(req)
		if err != nil {
			isSync = false
			return
		}
		defer resp.Body.Close()

		var bdb BDBResponse
		if err := json.NewDecoder(resp.Body).Decode(&bdb); err != nil {
			isSync = false
			return
		}

		isSync = isClusterSync(bdb.CRDTSources)
	})

	wg.Wait()

	return isHealthy, isSync
}

func isClusterSync(sources []CRDTSource) bool {
	if len(sources) == 0 {
		return true
	}

	for _, src := range sources {
		if src.Status != "in-sync" {
			return false
		}
	}
	return true
}

func (rw *RedisWrapper) AddClient(ctx context.Context, client *redis.Client, priority int, name string, clusterConfig *ClusterConfig) {
	isHealthy, isSync := rw.checkClusterAvailability(ctx, client, clusterConfig)

	newClient := &RedisClient{
		client:        client,
		priority:      priority,
		name:          name,
		clusterConfig: clusterConfig,
	}
	newClient.isHealthy.Store(isHealthy)
	newClient.isSync.Store(isSync)

	rw.mu.Lock()
	rw.clients = append(rw.clients, newClient)
	rw.mu.Unlock()
}

func (rw *RedisWrapper) getActiveClient() *RedisClient {
	rw.mu.RLock()
	defer rw.mu.RUnlock()
	return rw.activeClient
}

func (rw *RedisWrapper) resolveActiveClient() {
	rw.mu.Lock()
	defer rw.mu.Unlock()

	// Si el activo actual sigue healthy, solo ceder a uno con mejor priority Y sync
	if rw.activeClient != nil && rw.activeClient.isHealthy.Load() {
		for _, c := range rw.clients {
			if c != rw.activeClient && c.isHealthy.Load() && c.isSync.Load() && c.priority < rw.activeClient.priority {
				rw.activeClient = c
				return
			}
		}
		return
	}

	// No hay activo o el activo cayó: buscar el de menor priority healthy
	var best *RedisClient
	for _, c := range rw.clients {
		if c.isHealthy.Load() {
			if best == nil || c.priority < best.priority {
				best = c
			}
		}
	}
	rw.activeClient = best
}

func (rw *RedisWrapper) StartHealthCheck(ctx context.Context) {
	rw.checkClientsHealth(ctx)

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
			isHealthy, isSync := rw.checkClusterAvailability(ctx, c.client, c.clusterConfig)
			c.isHealthy.Store(isHealthy)
			c.isSync.Store(isSync)
		})
	}
	wg.Wait()

	rw.resolveActiveClient()
}

func (rw *RedisWrapper) Execute(ctx context.Context, fn func(*redis.Client) error) error {
	active := rw.getActiveClient()
	if active == nil {
		return errors.New("no healthy client available")
	}

	return fn(active.client)
}

func (rw *RedisWrapper) ExecuteWithFallback(ctx context.Context, fn func(*redis.Client) error) error {
	active := rw.getActiveClient()
	if active == nil {
		return errors.New("no healthy client available")
	}

	fmt.Printf("[ExecuteWithFallback] trying active: %s\n", active.name)

	err := fn(active.client)
	if err == nil {
		return nil
	}

	fmt.Printf("[ExecuteWithFallback] active failed: %v, trying fallback\n", err)

	// intentar en los demás healthy
	rw.mu.RLock()
	clients := rw.clients
	rw.mu.RUnlock()

	for _, c := range clients {
		if c == active || !c.isHealthy.Load() {
			continue
		}
		if err := fn(c.client); err == nil {
			return nil
		}
	}

	return err
}
