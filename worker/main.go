package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	red "github.com/gomodule/redigo/redis"

	"github.com/fzerorubigd/chapar/drivers/redis"
	"github.com/fzerorubigd/chapar/middlewares/storage"
	"github.com/fzerorubigd/chapar/workers"
)

var (
	redisServer = flag.String("redis-server", "127.0.0.1:6379", "redis server to connect to")
	prefix      = flag.String("prefix", "prefix_", "redis key prefix")
)

var sig = make(chan os.Signal, 4)

func cliContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	signal.Notify(sig, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGABRT)
	go func() {
		select {
		case <-sig:
			cancel()
		}
	}()

	return ctx
}

// A typical worker is like this

func main() {
	ctx := cliContext()
	flag.Parse()
	pool := &red.Pool{
		Dial: func() (red.Conn, error) {
			return red.Dial("tcp", *redisServer)
		},
		TestOnBorrow: func(c red.Conn, _ time.Time) error {
			_, err := c.Do("PING")
			return err
		},
		MaxIdle: 1,
	}
	// Create the driver for queue
	driver, err := redis.NewDriver(
		ctx,
		redis.WithQueuePrefix(*prefix),
		redis.WithRedisPool(pool),
	)
	if err != nil {
		log.Fatal(err)
	}

	// Create the manager
	m := workers.NewManager(driver, driver)
	// Register a global middleware
	m.RegisterMiddleware(
		middleware("Global"),
		storage.NewStorageMiddleware(&redisStorage{red: pool}),
	)
	// Register workers
	err = m.RegisterWorker("dummy", dummyWorker{}, workers.WithMiddleware(middleware("DummyMiddle")))
	if err != nil {
		log.Fatal(err)
	}
	// Process queues
	// this hangs until the context is done
	m.Process(ctx, workers.WithParallelLimit(10), workers.WithRetryCount(1))
}
