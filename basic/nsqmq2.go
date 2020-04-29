package basic

import (
	"context"
	"errors"
	"fmt"
	"github.com/nsqio/go-nsq"
	"gitlab.badanamu.com.cn/calmisland/imq/drive"
	"sync"
)

var(
	ErrHandleMessageFailed = errors.New("Handle message with error")
)

type NsqMQ2 struct {
	locker sync.Mutex
	consumerMap map[int]*nsq.Consumer
	curId int
}

func (n *NsqMQ2)Publish(ctx context.Context, topic string, message string) error{
	return drive.GetNSQProducer().Publish(topic, []byte(message))
}
func (n *NsqMQ2)Subscribe(topic string, handler func(ctx context.Context, message string)) int{
	consumer, err := drive.CreateNSQConsumer(topic, func(ctx context.Context, message string) error {
		handler(ctx, message)
		return nil
	})
	if err != nil{
		fmt.Println("Error:", err)
		return -1
	}
	n.locker.Lock()
	defer n.locker.Unlock()
	n.consumerMap[n.curId] = consumer

	n.curId ++
	return n.curId
}
func (n *NsqMQ2)SubscribeWithReconnect(topic string, handler func(ctx context.Context, message string) bool) int{
	consumer, err := drive.CreateNSQConsumer(topic, func(ctx context.Context, message string) error {
		ret := handler(ctx, message)
		if !ret {
			return ErrHandleMessageFailed
		}
		return nil
	})
	if err != nil{
		fmt.Println("Error:", err)
		return -1
	}

	n.locker.Lock()
	defer n.locker.Unlock()
	n.consumerMap[n.curId] = consumer
	n.curId ++
	return n.curId
}
func (n *NsqMQ2)Unsubscribe(hid int){
	n.locker.Lock()
	defer n.locker.Unlock()
	consumer, ok := n.consumerMap[hid]
	if ok {
		consumer.Stop()
		delete(n.consumerMap, hid)
	}
}

func NewNsqMQ2()*NsqMQ2{
	return &NsqMQ2{
		curId:      1,
		consumerMap: make(map[int]*nsq.Consumer),
	}
}