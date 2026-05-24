package redis

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

type Client struct{ rdb *goredis.Client }

func NewFromEnv() *Client {
	addr := strings.TrimSpace(os.Getenv("REDIS_ADDR"))
	if addr == "" {
		return nil
	}
	db := 0
	if raw := strings.TrimSpace(os.Getenv("REDIS_DB")); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil {
			db = parsed
		}
	}
	return &Client{rdb: goredis.NewClient(&goredis.Options{
		Addr:     addr,
		Password: os.Getenv("REDIS_PASSWORD"),
		DB:       db,
	})}
}

func (c *Client) Close() error {
	if c == nil || c.rdb == nil {
		return nil
	}
	return c.rdb.Close()
}

func (c *Client) Ping(ctx context.Context) error {
	if c == nil || c.rdb == nil {
		return nil
	}
	return c.rdb.Ping(ctx).Err()
}

func (c *Client) SetNX(ctx context.Context, key string, value any, ttl time.Duration) (bool, error) {
	return c.rdb.SetNX(ctx, key, value, ttl).Result()
}

func (c *Client) Get(ctx context.Context, key string) (string, error) {
	return c.rdb.Get(ctx, key).Result()
}

func (c *Client) Set(ctx context.Context, key string, value any, ttl time.Duration) error {
	return c.rdb.Set(ctx, key, value, ttl).Err()
}

func (c *Client) Incr(ctx context.Context, key string) (int64, error) {
	return c.rdb.Incr(ctx, key).Result()
}

func (c *Client) Expire(ctx context.Context, key string, ttl time.Duration) error {
	return c.rdb.Expire(ctx, key, ttl).Err()
}

func (c *Client) Del(ctx context.Context, key string) error {
	return c.rdb.Del(ctx, key).Err()
}

func (c *Client) SetJSON(ctx context.Context, key string, value any, ttl time.Duration) error {
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return c.Set(ctx, key, string(b), ttl)
}

func (c *Client) GetJSON(ctx context.Context, key string, dest any) error {
	raw, err := c.Get(ctx, key)
	if err != nil {
		return err
	}
	return json.Unmarshal([]byte(raw), dest)
}
