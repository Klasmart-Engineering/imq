package basic

import (
	"context"
	"fmt"
	"github.com/go-redis/redis"
	"gitlab.badanamu.com.cn/calmisland/common-cn/helper"
	"gitlab.badanamu.com.cn/calmisland/imq/drive"
	"sync"
	"time"
)

type RedisListMQ struct {
	sync.Mutex
	cid int
	quitMap map[int]chan struct{}
}


func (rmq *RedisListMQ) Publish(ctx context.Context, topic string, message string) error {
	publishMessage, err := marshalPublishMessage(ctx, message)
	if err != nil {
		return err
	}
	return drive.GetRedis().LPush(topic, publishMessage).Err()
}

func (rmq *RedisListMQ) startSubscribeLoop(handler func()) chan struct{}{
	quit :=  make(chan struct{})
	go func() {
		for {
			select {
			case <- quit:
				return
			default:
				handler()
			}
		}
	}()
	return quit
}

func (rmq *RedisListMQ) SubscribeWithReconnect(topic string, handler func(ctx context.Context, message string) error) int {
	rmq.Lock()
	defer rmq.Unlock()

	rmq.cid ++
	rmq.quitMap[rmq.cid] = rmq.startSubscribeLoop(func() {
		res := drive.GetRedis().BRPop(time.Second * 15, topic)
		result, err := res.Result()
		if err == redis.Nil{
			//Timeout
			return
		}
		if err != nil {
			fmt.Println("Receive message failed, error:", err)
			return
		}
		msg := result[len(result) - 1]
		go func() {
			publishMessage, err := unmarshalPublishMessage(msg)
			if err != nil {
				fmt.Println("Unmarshal message failed, error:", err)
				return
			}
			ctx := context.WithValue(context.Background(), helper.CtxKeyBadaCtx, publishMessage.BadaCtx)

			err = handler(ctx, publishMessage.Message)
			//若该消息未处理，则重新发送
			if err != nil {
				fmt.Println("Handle message with error: ", err)
				time.Sleep(requeue_delay)
				rmq.Publish(context.Background(), topic, msg)
			}
		}()
	})

	return rmq.cid
}

func (rmq *RedisListMQ) Subscribe(topic string, handler func(ctx context.Context, message string)) int {
	rmq.Lock()
	defer rmq.Unlock()
	rmq.cid ++

	rmq.quitMap[rmq.cid] = rmq.startSubscribeLoop(func() {
		res := drive.GetRedis().BRPop(time.Second * 15, topic)
		result, err := res.Result()
		if err == redis.Nil{
			//Timeout
			return
		}
		if err != nil {
			fmt.Println("Receive message failed, error:", err)
			return
		}
		msg := result[len(result) - 1]
		go func() {
			publishMessage, err := unmarshalPublishMessage(msg)
			if err != nil {
				fmt.Println("Unmarshal message failed, error:", err)
				return
			}
			ctx := context.WithValue(context.Background(), helper.CtxKeyBadaCtx, publishMessage.BadaCtx)
			handler(ctx, publishMessage.Message)
		}()
	})

	return rmq.cid
}

func (rmq *RedisListMQ) Unsubscribe(hid int) {
	rmq.Lock()
	defer rmq.Unlock()
	if rmq.quitMap[hid] != nil {
		rmq.quitMap[hid] <- struct{}{}
	}
}

func NewRedisListMQ() *RedisListMQ {
	return &RedisListMQ{
		quitMap: make(map[int]chan struct{}),
	}
}
