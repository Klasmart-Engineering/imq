package basic

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/segmentio/nsq-go"
	"gitlab.badanamu.com.cn/calmisland/common-cn/helper"
)

const (
	requeue_delay = time.Second * 5
)

type NSQConfig struct {
	Channel string
	Lookup  []string
	Address string
}

type NsqMQ struct {
	producerLock sync.Mutex
	consumerLock sync.Mutex
	producerMap  map[string]*nsq.Producer
	consumerMap  map[int]*nsq.Consumer
	config       NSQConfig
	curId        int
}

func (n *NsqMQ) tryGetPublisher(ctx context.Context, topic string) (*nsq.Producer, error) {
	producer, ok := n.producerMap[topic]
	if ok {
		return producer, nil
	}
	newProducer, err := nsq.StartProducer(nsq.ProducerConfig{
		Topic:   topic,
		Address: n.config.Address,
	})
	if err != nil {
		return nil, err
	}

	//Save map
	n.producerLock.Lock()
	defer n.producerLock.Unlock()
	n.producerMap[topic] = newProducer

	return newProducer, nil
}

func (n *NsqMQ) deleteProducer(ctx context.Context, topic string) {
	n.producerLock.Lock()
	defer n.producerLock.Unlock()

	producer := n.producerMap[topic]
	producer.Stop()
	delete(n.producerMap, topic)
}
func (n *NsqMQ) PendingMessage(ctx context.Context, topic string) ([]string, error){
	return nil, nil
}
func (n *NsqMQ) Publish(ctx context.Context, topic string, message string) error {
	// Starts a new producer that publishes to the TCP endpoint of a nsqd node.
	// The producer automatically handles connections in the background.
	producer, err := n.tryGetPublisher(ctx, topic)
	if err != nil {
		return err
	}

	// Publishes a message to the topic that this producer is configured for,
	// the method returns when the operation completes, potentially returning an
	// error if something went wrong.
	publishMessage, err := marshalPublishMessage(ctx, message)
	if err != nil {
		return err
	}
	err = producer.Publish([]byte(publishMessage))
	if err != nil {
		return err
	}

	// Stops the producer, all in-flight requests will be canceled and no more
	// messages can be published through this producer.
	//producer.Stop()
	return nil
}
func (n *NsqMQ) SubscribeWithReconnect(topic string, handler func(ctx context.Context, message string) bool) int {
	// Create a new consumer, looking up nsqd nodes from the listed nsqlookup
	// addresses, pulling messages from the 'world' channel of the 'hello' topic
	// with a maximum of 250 in-flight messages.
	consumer, err := nsq.StartConsumer(nsq.ConsumerConfig{
		Topic:       topic,
		Channel:     n.config.Channel,
		Lookup:      n.config.Lookup,
		MaxInFlight: 250,
	})
	if err != nil {
		return -1
	}

	go func() {
		// Consume messages, the consumer automatically connects to the nsqd nodes
		// it discovers and handles reconnections if something goes wrong.
		for msg0 := range consumer.Messages() {
			// handle the message, then call msg.Finish or msg.Requeue
			// ...
			go func(msg nsq.Message) {
				publishMessage, err := unmarshalPublishMessage(string(msg.Body))
				if err != nil {
					fmt.Println("Unmarshal message failed, error:", err)
					return
				}
				ctx := context.WithValue(context.Background(), helper.CtxKeyBadaCtx, publishMessage.BadaCtx)

				ret := handler(ctx, publishMessage.Message)
				if ret {
					msg.Finish()
				} else {
					//5秒后重试
					msg.Requeue(requeue_delay)
				}
			}(msg0)
		}
	}()

	n.consumerLock.Lock()
	defer n.consumerLock.Unlock()
	n.consumerMap[n.curId] = consumer
	id := n.curId
	n.curId++
	return id
}
func (n *NsqMQ) Subscribe(topic string, handler func(ctx context.Context, message string)) int {
	// Create a new consumer, looking up nsqd nodes from the listed nsqlookup
	// addresses, pulling messages from the 'world' channel of the 'hello' topic
	// with a maximum of 250 in-flight messages.
	consumer, err := nsq.StartConsumer(nsq.ConsumerConfig{
		Topic:       topic,
		Channel:     n.config.Channel,
		Lookup:      n.config.Lookup,
		MaxInFlight: 250,
	})
	if err != nil {
		return -1
	}

	go func() {
		// Consume messages, the consumer automatically connects to the nsqd nodes
		// it discovers and handles reconnections if something goes wrong.
		for msg := range consumer.Messages() {
			// handle the message, then call msg.Finish or msg.Requeue
			// ...
			go func() {
				fmt.Println("Get message:", msg)
				publishMessage, err := unmarshalPublishMessage(string(msg.Body))
				if err != nil {
					fmt.Println("Unmarshal message failed, error:", err)
					//return err
				}
				ctx := context.WithValue(context.Background(), helper.CtxKeyBadaCtx, publishMessage.BadaCtx)

				handler(ctx, publishMessage.Message)
				msg.Finish()
			}()

		}
	}()

	n.consumerLock.Lock()
	defer n.consumerLock.Unlock()
	n.consumerMap[n.curId] = consumer
	id := n.curId
	n.curId++
	return id
}

func (n *NsqMQ) Unsubscribe(hid int) {
	n.consumerLock.Lock()
	defer n.consumerLock.Unlock()
	consumer := n.consumerMap[hid]
	consumer.Stop()
	delete(n.consumerMap, hid)
}

//producerLock sync.Mutex
//consumerLock sync.Mutex
//producerMap  map[string]*nsq.Producer
//consumerMap  map[int]*nsq.Consumer
//config       *drive.NSQConfig
func NewNSQMQ(config NSQConfig) *NsqMQ {
	return &NsqMQ{
		curId:       1,
		producerMap: make(map[string]*nsq.Producer),
		consumerMap: make(map[int]*nsq.Consumer),
		config:      config,
	}
}
