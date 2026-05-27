package redisstreams

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type redisClient interface {
	XGroupCreateMkStream(ctx context.Context, stream string, group string, start string) *redis.StatusCmd
	XReadGroup(ctx context.Context, a *redis.XReadGroupArgs) *redis.XStreamSliceCmd
	XAck(ctx context.Context, stream string, group string, ids ...string) *redis.IntCmd
}

type TaskHandler interface {
	Submit(ctx context.Context, id string, payload map[string]string, ack func() error) error
}

type StreamOptions struct {
	BatchSize    int64
	BlockTimeout time.Duration
	ErrHandler   func(err error)
}

func DefaultStreamOptions() StreamOptions {
	return StreamOptions{
		BatchSize:    10,
		BlockTimeout: 2 * time.Second,
		ErrHandler: func(err error) {
			log.Printf("[redispool] stream error: %v", err)
		},
	}
}

type StreamAdapter struct {
	rdb        redisClient
	streamName string
	groupName  string
	consumerID string
	opts       StreamOptions
}

func NewStreamAdapter(rdb redisClient, stream, group, consumer string, opts StreamOptions) *StreamAdapter {
	if opts.BatchSize <= 0 {
		opts.BatchSize = DefaultStreamOptions().BatchSize
	}
	if opts.BlockTimeout <= 0 {
		opts.BlockTimeout = DefaultStreamOptions().BlockTimeout
	}
	if opts.ErrHandler == nil {
		opts.ErrHandler = DefaultStreamOptions().ErrHandler
	}

	return &StreamAdapter{
		rdb:        rdb,
		streamName: stream,
		groupName:  group,
		consumerID: consumer,
		opts:       opts,
	}
}

func (a *StreamAdapter) Initialize(ctx context.Context, offset string) error {
	if err := a.rdb.XGroupCreateMkStream(ctx, a.streamName, a.groupName, offset).Err(); err != nil {
		if strings.HasPrefix(err.Error(), "BUSYGROUP") {
			return nil
		}
		return fmt.Errorf("could not create group stream: %w", err)
	}
	return nil
}

// Consume replaces FetchTasks injecting tasks directly into the handler.
func (a *StreamAdapter) Consume(ctx context.Context, handler TaskHandler) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			streams, err := a.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    a.groupName,
				Consumer: a.consumerID,
				Streams:  []string{a.streamName, ">"},
				Count:    a.opts.BatchSize,
				Block:    a.opts.BlockTimeout,
			}).Result()
			if err != nil {
				if errors.Is(err, redis.Nil) {
					continue
				}
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return
				}
				a.opts.ErrHandler(err)

				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Second):
				}
				continue
			}

			for _, stream := range streams {
				for _, msg := range stream.Messages {
					msgID := msg.ID

					payload := make(map[string]string)
					for k, v := range msg.Values {
						if str, ok := v.(string); ok {
							payload[k] = str
						}
					}

					if err := handler.Submit(ctx, msg.ID, payload, func() error {
						ackCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
						defer cancel()
						return a.rdb.XAck(ackCtx, a.streamName, a.groupName, msgID).Err()
					}); err != nil {
						a.opts.ErrHandler(fmt.Errorf("could not submit task to handler: %w", err))
					}
				}
			}
		}
	}
}
