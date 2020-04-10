package basic

import (
	"gitlab.badanamu.com.cn/calmisland/imq/drive"
	"github.com/go-redis/redis"
	"sync"
)

type RedisMQ struct {
	lock sync.Mutex
	curId int
	subHandler map[int]*redis.PubSub
}

func(rmq *RedisMQ)Publish(topic string, message string) error{
	//drive.GetRedis().RPush(topic, message)
	return drive.GetRedis().Publish(topic, message).Err()
}

func(rmq *RedisMQ)Subscribe(topic string, handler func(message string)) int{
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
			handler(msg.String())
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