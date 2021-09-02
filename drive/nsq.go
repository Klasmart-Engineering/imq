package drive

import (
	"context"
	"errors"
	"log"

	nsq "github.com/nsqio/go-nsq"
)

type NSQConfig struct {
	Address string
	Lookup  []string
	Channel string
}

var (
	ErrNoNSQConfig = errors.New("No nsq config")
)

var producer *nsq.Producer
var config *NSQConfig

func OpenNSQProducer() error {
	tmpProducer, err := nsq.NewProducer(config.Address, nsq.NewConfig())
	if err != nil {
		return err
	}
	err = tmpProducer.Ping()
	if err != nil {
		tmpProducer.Stop()
		tmpProducer = nil
		log.Println(err.Error())
		return err
	}
	producer = tmpProducer
	return nil
}

func SetNSQConfig(c *NSQConfig) {
	config = c
}

func GetNSQProducer() *nsq.Producer {
	return producer
}

type ConsumerHandler struct {
	HandleMessageFunc func(ctx context.Context, message string) error
}

func (c *ConsumerHandler) HandleMessage(msg *nsq.Message) error {
	return c.HandleMessageFunc(context.Background(), string(msg.Body))
}

func CreateNSQConsumer(topic string, handler func(ctx context.Context, message string) error) (*nsq.Consumer, error) {
	if config == nil {
		return nil, ErrNoNSQConfig
	}
	consumer, err := nsq.NewConsumer(topic, config.Channel, nsq.NewConfig())
	if err != nil {
		return nil, err
	}
	consumer.AddHandler(&ConsumerHandler{
		HandleMessageFunc: handler,
	})
	if len(config.Lookup) > 0 {
		err = consumer.ConnectToNSQLookupds(config.Lookup)
		if err != nil {
			return nil, err
		}
	} else {
		err = consumer.ConnectToNSQD(config.Address)
		if err != nil {
			return nil, err
		}
	}
	return consumer, nil
}
