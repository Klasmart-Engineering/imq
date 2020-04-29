package imq

import (
	"context"
	"gitlab.badanamu.com.cn/calmisland/imq/basic"
	"gitlab.badanamu.com.cn/calmisland/imq/drive"
	"errors"
)

var(
	ErrUnknownDrive = errors.New("Unknown mq drive")
	ErrInvalidNSQConfig = errors.New("Invalid nsq config")
)

type IMessageQueue interface{
	Publish(ctx context.Context, topic string, message string) error
	Subscribe(topic string, handler func(ctx context.Context, message string)) int
	SubscribeWithReconnect(topic string, handler func(ctx context.Context, message string) error) int
	Unsubscribe(hid int)
}

type Config struct {
	Drive string

	RedisHost string
	RedisPort int
	RedisPassword string

	NSQChannel string
	NSQLookup []string
	NSQAddress string
	OpenProducer bool
}

func CreateMessageQueue(conf Config) (IMessageQueue, error) {
	switch conf.Drive {
	case "redis":
		err := drive.OpenRedis(conf.RedisHost, conf.RedisPort, conf.RedisPassword)
		if err != nil {
			return nil, err
		}
		return basic.NewRedisMQ(), nil
	case "nsq":
		if conf.NSQLookup == nil || conf.NSQChannel == "" || conf.NSQAddress == ""{
			return nil, ErrInvalidNSQConfig
		}
		drive.SetNSQConfig(&drive.NSQConfig{
			Address: conf.NSQAddress,
			Lookup:  conf.NSQLookup,
			Channel: conf.NSQChannel,
		})
		if conf.OpenProducer{
			err := drive.OpenNSQProducer()
			if err != nil {
				return nil, err
			}
		}
		return basic.NewNsqMQ2(), nil
	}
	return nil, ErrUnknownDrive
}
