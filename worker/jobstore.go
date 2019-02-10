package main

import (
	"context"

	"github.com/gomodule/redigo/redis"

	"github.com/fzerorubigd/chapar/tasks"
)

type redisStorage struct {
	red *redis.Pool
}

// Store is an example of store. mostly you do not want to return err that easily
// also maybe better storage (like database?) or ...
func (s *redisStorage) Store(ctx context.Context, task *tasks.Task, e error) (es error) {
	d, err := task.Marshal()
	if err != nil {
		return err // maybe just log?
	}
	c := s.red.Get()

	_, err = c.Do("HSET", task.ID.String(), "TASK", string(d))
	if err != nil {
		return err
	}
	if e != nil {
		_, err = c.Do("HSET", task.ID.String(), "ERR", e.Error())
		if err != nil {
			return err
		}
		_, err = c.Do("HINCRBY", task.ID.String(), "REDELIVER", "1")
		if err != nil {
			return err
		}
	}

	// lets set the time for 3 days to not bloat the server
	_, err = c.Do("EXPIRE", task.ID.String(), 72*60*60)
	return err
}
