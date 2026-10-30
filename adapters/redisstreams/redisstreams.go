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

// Message is a single entry read from the stream. Ack acknowledges the
// message back to the stream; call it only after the work has succeeded.
type Message struct {
	ID     string
	Values map[string]string
	Ack    func() error
}

// Handler processes one stream message. With a workerpool.Pool, a typical
// implementation wraps the message in a Task and calls Do/TryDo, acking on
// success:
//
//	_, err := pool.Do(ctx, myTask{msg: msg})
//	if err == nil {
//		return msg.Ack()
//	}
//	return err
type Handler interface {
	Handle(ctx context.Context, msg Message) error
}

// HandlerFunc adapts a function to the Handler interface.
type HandlerFunc func(ctx context.Context, msg Message) error

// Handle calls f(ctx, msg).
func (f HandlerFunc) Handle(ctx context.Context, msg Message) error { return f(ctx, msg) }

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

// Consume reads messages from the stream via the consumer group and passes
// each to handler.Handle. It invokes the handler synchronously, one message at
// a time, and blocks until ctx is cancelled — a handler that waits on
// pool.Do therefore applies backpressure to consumption.
//
// Limitations:
//   - No pending-entry reclaim: if the consumer crashes after XReadGroup but
//     before msg.Ack runs, the message stays in the group's pending entries
//     list (PEL) indefinitely. Use XAUTOCLAIM/XPENDING externally to
//     reclaim stale messages from dead consumers.
//   - Only string field values are surfaced in Message.Values; non-string
//     values (numbers, booleans, nested objects) are silently dropped.
func (a *StreamAdapter) Consume(ctx context.Context, handler Handler) {
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

					ack := func() error {
						ackCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
						defer cancel()
						return a.rdb.XAck(ackCtx, a.streamName, a.groupName, msgID).Err()
					}
					if err := handler.Handle(ctx, Message{ID: msgID, Values: payload, Ack: ack}); err != nil {
						a.opts.ErrHandler(fmt.Errorf("handler failed for message %s: %w", msgID, err))
					}
				}
			}
		}
	}
}
