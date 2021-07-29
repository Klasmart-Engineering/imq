package basic

import (
	"context"
	"fmt"
	"sync"

	"github.com/nsqio/go-nsq"
	"gitlab.badanamu.com.cn/calmisland/common-cn/helper"
	"gitlab.badanamu.com.cn/calmisland/imq/drive"
)

type NsqMQ2 struct {
	locker      sync.Mutex
	consumerMap map[int]*nsq.Consumer
	curId       int
}

func (n *NsqMQ2) Publish(ctx context.Context, topic string, message string) error {
	publishMessage, err := marshalPublishMessage(ctx, message)
	if err != nil {
		return err
	}
	return drive.GetNSQProducer().Publish(topic, []byte(publishMessage))
}
func (n *NsqMQ2) Subscribe(topic string, handler func(ctx context.Context, message string)) int {
	consumer, err := drive.CreateNSQConsumer(topic, func(ctx context.Context, message string) error {
		publishMessage, err := unmarshalPublishMessage(message)
		if err != nil {
			fmt.Println("Unmarshal message failed, error:", err)
			return err
		}
		ctx0 := context.WithValue(ctx, helper.CtxKeyBadaCtx, publishMessage.BadaCtx)

		handler(ctx0, publishMessage.Message)
		return nil
	})
	if err != nil {
		fmt.Println("Error:", err)
		return -1
	}
	n.locker.Lock()
	defer n.locker.Unlock()
	n.consumerMap[n.curId] = consumer

	id := n.curId
	n.curId++
	return id
}
func (n *NsqMQ2) SubscribeWithReconnect(topic string, handler func(ctx context.Context, message string) error) int {
	consumer, err := drive.CreateNSQConsumer(topic, func(ctx context.Context, message string) error {
		publishMessage, err := unmarshalPublishMessage(message)
		if err != nil {
			fmt.Println("Unmarshal message failed, error:", err)
			return err
		}
		ctx0 := context.WithValue(ctx, helper.CtxKeyBadaCtx, publishMessage.BadaCtx)

		err = handler(ctx0, publishMessage.Message)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		fmt.Println("Error:", err)
		return -1
	}

	n.locker.Lock()
	defer n.locker.Unlock()
	n.consumerMap[n.curId] = consumer
	id := n.curId
	n.curId++
	return id
}
func (n *NsqMQ2) Unsubscribe(hid int) {
	n.locker.Lock()
	defer n.locker.Unlock()
	consumer, ok := n.consumerMap[hid]
	if ok {
		consumer.Stop()
		delete(n.consumerMap, hid)
	}
}

func NewNsqMQ2() *NsqMQ2 {
	return &NsqMQ2{
		curId:       1,
		consumerMap: make(map[int]*nsq.Consumer),
	}
}
