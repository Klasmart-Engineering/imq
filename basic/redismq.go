package basic

import (
	"context"
	"fmt"
	"github.com/go-redis/redis"
	"gitlab.badanamu.com.cn/calmisland/imq/drive"
	"sync"
	"time"
)

type RedisMQ struct {
	lock sync.Mutex
	curId int
	subHandler map[int]*redis.PubSub
}

type PublishMessage struct {
	Message string `json:"message"`
}

func(rmq *RedisMQ)Publish(ctx context.Context, topic string, message string) error{
	return drive.GetRedis().Publish(topic, message).Err()
}
func(rmq *RedisMQ)SubscribeWithReconnect(topic string, handler func(ctx context.Context, message string) error) int{
	sub := drive.GetRedis().Subscribe(topic)

	go func() {
		for {
			//msg := <- sub.Channel()
			msg, err := sub.ReceiveMessage()
			if err != nil{
				return
			}

			err = handler(context.Background(), msg.Payload)
			//若该消息未处理，则重新发送
			if err != nil {
				fmt.Println("Handle message with error: ", err)
				time.Sleep(requeue_delay)
				rmq.Publish(context.Background(), topic, msg.Payload)
			}
		}
	}()

	rmq.lock.Lock()
	defer rmq.lock.Unlock()
	rmq.curId ++
	rmq.subHandler[rmq.curId] = sub
	return rmq.curId
}

func(rmq *RedisMQ)Subscribe(topic string, handler func(ctx context.Context, message string)) int{
	sub := drive.GetRedis().Subscribe(topic)
	go func() {
		for {
			msg, err := sub.ReceiveMessage()
			if err != nil{
				return
			}

			handler(context.Background(), msg.Payload)
		}
	}()

	rmq.lock.Lock()
	defer rmq.lock.Unlock()
	rmq.curId ++
	rmq.subHandler[rmq.curId] = sub
	return rmq.curId
}

func (rmq *RedisMQ) Unsubscribe(hid int) {
	rmq.lock.Lock()
	defer rmq.lock.Unlock()
	sub, ok := rmq.subHandler[hid]
	if ok {
		sub.Close()
	}
}

func NewRedisMQ()*RedisMQ{
	return &RedisMQ{
		curId:      1,
		subHandler: make(map[int]*redis.PubSub),
	}
}