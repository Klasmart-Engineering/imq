package basic

import (
	"context"
	"fmt"
	"gitlab.badanamu.com.cn/calmisland/imq/drive"
	"testing"
	"time"
)

func subscribeHandler(ctx context.Context, msg string) {
	fmt.Println("1:Receive message:", msg)
}
func subscribeHandler2(ctx context.Context, msg string) {
	fmt.Println("2:Receive message:", msg)
}

func TestSubscribePublish(t *testing.T) {
	err := drive.OpenRedis("127.0.0.1", 6379, "")
	if err != nil{
		fmt.Println(err)
		return
	}

	mq := NewRedisMQ("./failedlist.json")
	h1 := mq.Subscribe("mypay", subscribeHandler)
	mq.Subscribe("mypay", subscribeHandler2)

	mq.Publish(context.Background(), "mypay", "Goodbye1")
	mq.Publish(context.Background(), "mypay", "Goodbye2")
	mq.Publish(context.Background(), "mypay", "Goodbye3")
	mq.Unsubscribe(h1)
	mq.Publish(context.Background(), "mypay", "Goodbye4")
	mq.Publish(context.Background(), "mypay", "Goodbye5")

	time.Sleep(time.Second)
}



func TestSubscribePublishTimeout(t *testing.T) {
	err := drive.OpenRedis("127.0.0.1", 6379, "")
	if err != nil{
		fmt.Println(err)
		return
	}

	mq := NewRedisMQ("failed_list.json")
	mq.Subscribe("mypay", subscribeHandler)
	mq.Subscribe("mypay", subscribeHandler2)

	mq.Publish(context.Background(), "mypay", "Goodbye1")
	mq.Publish(context.Background(), "mypay", "Goodbye2")
	mq.Publish(context.Background(), "mypay", "Goodbye3")

	time.Sleep(time.Minute * 5)

	mq.Publish(context.Background(), "mypay", "timeout goodbye")


	time.Sleep(time.Minute * 1)

	mq.Publish(context.Background(), "mypay", "timeout goodbye222")
}
