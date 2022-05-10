package basic

import (
	"context"
	"testing"
	"time"

	"github.com/KL-Engineering/imq/drive"
)

func TestNSQSubscribePublish(t *testing.T) {

	mq := NewNSQMQ(NSQConfig{
		Channel: "Hello",
		Lookup:  []string{"127.0.0.1:4161"},
		Address: "127.0.0.1:4150",
	})
	mq.Subscribe("mypay", subscribeHandler)
	mq.Subscribe("mypay", subscribeHandler2)

	mq.Publish(context.Background(), "mypay", "Goodbye1")
	mq.Publish(context.Background(), "mypay", "Goodbye2")
	mq.Publish(context.Background(), "mypay", "Goodbye3")
	//mq.Unsubscribe(h1)
	mq.Publish(context.Background(), "mypay", "Goodbye4")
	mq.Publish(context.Background(), "mypay", "Goodbye5")

	time.Sleep(time.Second * 5)
}

func TestNSQSubscribePublish2(t *testing.T) {
	drive.SetNSQConfig(&drive.NSQConfig{
		Channel: "Hello",
		//Lookup:  []string{"127.0.0.1:4160"},
		Address: "127.0.0.1:4150",
	})
	err := drive.OpenNSQProducer()
	if err != nil {
		panic(err)
	}
	mq := NewNsqMQ2()
	mq.Subscribe("mypay", subscribeHandler)
	mq.Subscribe("mypay", subscribeHandler2)

	mq.Publish(context.Background(), "mypay", "Goodbye1")
	mq.Publish(context.Background(), "mypay", "Goodbye2")
	mq.Publish(context.Background(), "mypay", "Goodbye3")
	//mq.Unsubscribe(h1)
	mq.Publish(context.Background(), "mypay", "Goodbye4")
	mq.Publish(context.Background(), "mypay", "Goodbye5")

	time.Sleep(time.Second * 5)
}
