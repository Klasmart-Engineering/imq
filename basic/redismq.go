package basic

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/go-redis/redis"
	"gitlab.badanamu.com.cn/calmisland/common-cn/helper"
	"gitlab.badanamu.com.cn/calmisland/imq/drive"
)

type RedisMQ struct {
	lock       sync.Mutex
	curId      int
	subHandler map[int]*redis.PubSub
}

type PublishMessage struct {
	Message string          `json:"message"`
	BadaCtx *helper.BadaCtx `json:"bada_ctx"`
}

func marshalPublishMessage(ctx context.Context, message string) (string, error) {
	badaCtx, _ := ctx.Value(helper.CtxKeyBadaCtx).(*helper.BadaCtx)
	publishMessage := PublishMessage{
		Message: message,
		BadaCtx: badaCtx,
	}
	resp, err := json.Marshal(publishMessage)
	if err != nil {
		return "", err
	}
	return string(resp), nil
}

func unmarshalPublishMessage(message string) (*PublishMessage, error) {
	publishMessage := new(PublishMessage)
	err := json.Unmarshal([]byte(message), publishMessage)
	if err != nil {
		return nil, err
	}
	return publishMessage, nil
}

func (rmq *RedisMQ) Publish(ctx context.Context, topic string, message string) error {
	publishMessage, err := marshalPublishMessage(ctx, message)
	if err != nil {
		return err
	}
	return drive.GetRedis().Publish(topic, publishMessage).Err()
}
func (rmq *RedisMQ) SubscribeWithReconnect(topic string, handler func(ctx context.Context, message string) error) int {
	sub := drive.GetRedis().Subscribe(topic)

	go func() {
		for {
			//msg := <- sub.Channel()
			msg, err := sub.ReceiveMessage()
			if err != nil {
				fmt.Println("Receive message failed, error:", err)
				continue
			}
			go func() {
				publishMessage, err := unmarshalPublishMessage(msg.Payload)
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
					rmq.Publish(context.Background(), topic, msg.Payload)
				}
			}()
		}
	}()

	rmq.lock.Lock()
	defer rmq.lock.Unlock()
	id := rmq.curId
	rmq.curId++
	rmq.subHandler[rmq.curId] = sub
	return id
}

func (rmq *RedisMQ) Subscribe(topic string, handler func(ctx context.Context, message string)) int {
	sub := drive.GetRedis().Subscribe(topic)
	go func() {
		for {
			msg, err := sub.ReceiveMessage()
			if err != nil {
				fmt.Println("Receive message failed, error:", err)
				continue
			}
			go func() {
				publishMessage, err := unmarshalPublishMessage(msg.Payload)
				if err != nil {
					fmt.Println("Unmarshal message failed, error:", err)
					return
				}
				ctx := context.WithValue(context.Background(), helper.CtxKeyBadaCtx, publishMessage.BadaCtx)

				handler(ctx, publishMessage.Message)
			}()
		}
	}()

	rmq.lock.Lock()
	defer rmq.lock.Unlock()
	id := rmq.curId
	rmq.curId++
	rmq.subHandler[rmq.curId] = sub
	return id
}

func (rmq *RedisMQ) Unsubscribe(hid int) {
	rmq.lock.Lock()
	defer rmq.lock.Unlock()
	sub, ok := rmq.subHandler[hid]
	if ok {
		sub.Close()
	}
}

func NewRedisMQ() *RedisMQ {
	return &RedisMQ{
		curId:      1,
		subHandler: make(map[int]*redis.PubSub),
	}
}
