package redispkg

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

var ErrKeyAlreadyExists = errors.New("Key already exists")

func AddNewKey(ctx context.Context, rds *redis.Client, key string, value any) error {
	created, err := rds.SetNX(ctx, key, value, 600*time.Second).Result()
	if err != nil {
		fmt.Println("Error while setting new key")
		return err
	}
	if created != true {
		fmt.Println("Key already exists. Deduplication mechanism used!")
		return ErrKeyAlreadyExists
	}

	fmt.Printf("New key-value pair was set. Key: %s\n", key)
	return nil
}

func GetKey(ctx context.Context, rds *redis.Client, key string) {
	ans, err := rds.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			fmt.Println("Cache miss")
		} else {
			panic(err)
		}
	}
	fmt.Printf("Cache hit: %s = %s\n", key, ans)

	ttl, err := rds.TTL(ctx, key).Result()
	if err != nil {
		panic(err)
	}
	fmt.Printf("TTL for this message: %v\n", ttl)

}
