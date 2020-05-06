package basic

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestKafkaSubscribePublish(t *testing.T) {
	mq := NewKafkaMQ(KafkaConfig{
		BootstrapAddress: []string{"192.168.1.234:19092"},
		GroupId:          "group.test",
	})

	mq.Subscribe("mypay", subscribeHandler)
	id := mq.Subscribe("mypay", subscribeHandler2)

	mq.Publish(context.Background(), "mypay", "Goodbye1")
	mq.Publish(context.Background(), "mypay", "Goodbye2")
	mq.Publish(context.Background(), "mypay", "Goodbye3")
	fmt.Println(id)

	mq.Unsubscribe(id)
	//mq.Unsubscribe(h1)
	mq.Publish(context.Background(), "mypay", "Goodbye4")
	mq.Publish(context.Background(), "mypay", "Goodbye5")

	time.Sleep(time.Second * 60)
}
