package basic

import (
	"calmisland/imq/drive"
	"fmt"
	"testing"
)

func subscribeHandler(msg string) {
	fmt.Println("1:Receive message:", msg)
}
func subscribeHandler2(msg string) {
	fmt.Println("2:Receive message:", msg)
}

func TestSubscribePublish(t *testing.T) {
	err := drive.OpenRedis("127.0.0.1", 6379, "")
	if err != nil{
		fmt.Println(err)
		return
	}

	mq := NewRedisMQ()
	h1 := mq.Subscribe("mypay", subscribeHandler)
	mq.Subscribe("mypay", subscribeHandler2)

	mq.Publish("mypay", "Goodbye1")
	mq.Publish("mypay", "Goodbye2")
	mq.Publish("mypay", "Goodbye3")
	mq.Unsubscribe(h1)
	mq.Publish("mypay", "Goodbye4")
	mq.Publish("mypay", "Goodbye5")
}
