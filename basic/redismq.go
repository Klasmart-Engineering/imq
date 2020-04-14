package basic

import (
	"context"
	"encoding/json"
	"gitlab.badanamu.com.cn/calmisland/imq/drive"
	"github.com/go-redis/redis"
	"sync"
)

type RedisMQ struct {
	lock sync.Mutex
	curId int
	subHandler map[int]*redis.PubSub
}

type PublishMessage struct {
	Ctx context.Context `json:"ctx"`
	Message string `json:"message"`
}

func(rmq *RedisMQ)Publish(ctx context.Context, topic string, message string) error{
	//drive.GetRedis().RPush(topic, message)
	msg := PublishMessage{
		Ctx: ctx,
		Message: message,
	}
	msgJSON, err := json.Marshal(msg)
	if err != nil{
		return err
	}
	return drive.GetRedis().Publish(topic, string(msgJSON)).Err()
}

func(rmq *RedisMQ)Subscribe(topic string, handler func(ctx context.Context, message string)) int{
	//drive.GetRedis().BLPop(time.Minute, topic)
	rmq.lock.Lock()
	defer rmq.lock.Unlock()
	sub := drive.GetRedis().Subscribe(topic)
	go func() {
		for {
			msg, err := sub.ReceiveMessage()
			if err != nil{
				return
			}

			msgJSON := msg.String()
			publishMessage := new(PublishMessage)
			err = json.Unmarshal([]byte(msgJSON), publishMessage)
			if err != nil{
				return
			}

			handler(publishMessage.Ctx, publishMessage.Message)
		}
	}()
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