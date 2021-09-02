package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"gitlab.badanamu.com.cn/calmisland/imq"
)

func TestPublishNSQ(t *testing.T) {
	mq, err := imq.CreateMessageQueue(imq.Config{
		Drive:        "nsq",
		NSQChannel:   "ch0",
		NSQLookup:    []string{},
		NSQAddress:   "127.0.0.1:4150",
		OpenProducer: true,
	})
	if err != nil {
		panic(err)
	}
	sid := mq.SubscribeWithReconnect("hello1", func(ctx context.Context, message string) error {
		fmt.Println("Get message:", message)
		return nil
	})
	fmt.Println(sid)

	err = mq.Publish(context.Background(), "hello1", "444")
	if err != nil {
		panic(err)
	}
	time.Sleep(time.Second * 2)
}
