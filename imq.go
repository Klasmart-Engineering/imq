package imq

import (
	"context"
	"gitlab.badanamu.com.cn/calmisland/imq/basic"
	"gitlab.badanamu.com.cn/calmisland/imq/drive"
	"errors"
)

var(
	ErrUnknownDrive = errors.New("Unknown mq drive")
)

type IMessageQueue interface{
	Publish(ctx context.Context, topic string, message string) error
	Subscribe(topic string, handler func(ctx context.Context, message string)) int
	Unsubscribe(hid int)
}

type Config struct {
	Drive string

	RedisHost string
	RedisPort int
	RedisPassword string
}

func CreateMessageQueue(conf Config) (IMessageQueue, error) {
	switch conf.Drive {
	case "redis":
		err := drive.OpenRedis(conf.RedisHost, conf.RedisPort, conf.RedisPassword)
		if err != nil {
			return nil, err
		}
		return basic.NewRedisMQ(), nil
	}
	return nil, ErrUnknownDrive
}
