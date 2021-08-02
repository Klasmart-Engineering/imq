package basic

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/segmentio/kafka-go"
	"gitlab.badanamu.com.cn/calmisland/common-cn/helper"
	"gitlab.badanamu.com.cn/calmisland/imq/drive"
)

type KafkaConfig struct {
	BootstrapAddress []string
	GroupId          string
}

type KafkaMQ struct {
	locker       sync.Mutex
	producerLock sync.Mutex
	curId        int
	consumerMap  map[int]*kafka.Reader
	producerMap  map[string]*kafka.Writer

	config KafkaConfig
}

func (n *KafkaMQ) tryGetPublisher(ctx context.Context, topic string) (*kafka.Writer, error) {
	producer, ok := n.producerMap[topic]
	if ok {
		return producer, nil
	}
	newProducer := drive.NewKafkaWriter(topic, n.config.BootstrapAddress)

	//Save map
	n.producerLock.Lock()
	defer n.producerLock.Unlock()
	n.producerMap[topic] = newProducer

	return newProducer, nil
}

func (k *KafkaMQ) deleteProducer(ctx context.Context, topic string) {
	k.producerLock.Lock()
	defer k.producerLock.Unlock()

	producer := k.producerMap[topic]
	producer.Close()
	delete(k.producerMap, topic)
}

func (k *KafkaMQ) Publish(ctx context.Context, topic string, message string) error {
	publishMessage, err := marshalPublishMessage(ctx, message)
	if err != nil {
		return err
	}

	publisher, err := k.tryGetPublisher(ctx, topic)
	if err != nil {
		return err
	}

	err = publisher.WriteMessages(ctx, kafka.Message{
		Key:   []byte(topic),
		Value: []byte(publishMessage),
	})
	if err != nil {
		return err
	}
	return nil
}
func (k *KafkaMQ) Subscribe(topic string, handler func(ctx context.Context, message string)) int {
	consumer := drive.NewKafkaReader(topic, k.config.BootstrapAddress, k.config.GroupId)
	go func() {
		for {
			msg0, err := consumer.ReadMessage(context.Background())
			if err == nil {
				go func(msg kafka.Message) {
					publishMessage, err := unmarshalPublishMessage(string(msg.Value))
					if err != nil {
						fmt.Println("Unmarshal message failed, error:", err)
						//return err
					}
					ctx := context.WithValue(context.Background(), helper.CtxKeyBadaCtx, publishMessage.BadaCtx)
					handler(ctx, publishMessage.Message)
					fmt.Printf("Message on %s: %s\n", msg.Key, string(msg.Value))
				}(msg0)
			} else {
				// The client will automatically try to recover from all errors.
				fmt.Printf("Consumer error: %v (%v)\n", err, msg0)
				//连接已关闭
				if err == io.EOF {
					break
				}
			}
		}
	}()

	k.locker.Lock()
	defer k.locker.Unlock()
	k.consumerMap[k.curId] = consumer
	id := k.curId
	k.curId++

	return id
}
func (k *KafkaMQ) PendingMessage(ctx context.Context, topic string) ([]string, error){
	return nil, nil
}
func (k *KafkaMQ) SubscribeWithReconnect(topic string, handler func(ctx context.Context, message string) error) int {
	consumer := drive.NewKafkaReader(topic, k.config.BootstrapAddress, k.config.GroupId)
	go func() {
		for {
			msg0, err := consumer.ReadMessage(context.Background())
			if err == nil {
				go func(msg kafka.Message) {
					publishMessage, err := unmarshalPublishMessage(string(msg.Value))
					if err != nil {
						fmt.Println("Unmarshal message failed, error:", err)
						//return err
					}
					ctx := context.WithValue(context.Background(), helper.CtxKeyBadaCtx, publishMessage.BadaCtx)
					err = handler(ctx, publishMessage.Message)
					if err != nil {
						fmt.Printf("Consumer error: %v (%v)\n", err, msg)
					} else {
						fmt.Printf("Message on %s: %s\n", msg.Key, string(msg.Value))
					}
				}(msg0)

			} else {
				// The client will automatically try to recover from all errors.
				fmt.Printf("Consumer error: %v (%v)\n", err, msg0)
				if err == io.EOF {
					break
				}
			}
		}
	}()

	k.locker.Lock()
	defer k.locker.Unlock()
	k.consumerMap[k.curId] = consumer
	id := k.curId
	k.curId++
	return id
}
func (k *KafkaMQ) Unsubscribe(hid int) {
	k.locker.Lock()
	defer k.locker.Unlock()
	consumer, ok := k.consumerMap[hid]
	if ok {
		fmt.Println("Closing consumer: ", hid)
		consumer.Close()
		delete(k.consumerMap, hid)
	}
}

func NewKafkaMQ(config KafkaConfig) *KafkaMQ {
	return &KafkaMQ{
		curId:       1,
		consumerMap: make(map[int]*kafka.Reader),
		producerMap: make(map[string]*kafka.Writer),
		config:      config,
	}
}
