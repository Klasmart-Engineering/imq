package basic

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/KL-Engineering/imq/drive"
	"github.com/KL-Engineering/imq/failedlist"
	"github.com/KL-Engineering/tracecontext"
	"github.com/go-redis/redis"
)

type RedisMQ struct {
	lock       sync.Mutex
	curId      int
	subHandler map[int]*redis.PubSub

	recorder                *failedlist.Recorder
	recordOnce              sync.Once
	recorderPersistencePath string
}

type PublishMessage struct {
	Message string                    `json:"message"`
	BadaCtx tracecontext.TraceContext `json:"bada_ctx"`
}

func marshalPublishMessage(ctx context.Context, message string) (string, error) {
	badaCtx, ok := tracecontext.GetTraceContext(ctx)

	if !ok || badaCtx == nil {
		*badaCtx = tracecontext.NewTraceContext()
	}
	publishMessage := PublishMessage{
		Message: message,
		BadaCtx: *badaCtx,
	}
	resp, err := json.Marshal(publishMessage)
	if err != nil {
		return "", err
	}
	return string(resp), nil
}

func unmarshalPublishMessage(message string) (*PublishMessage, error) {
	publishMessage := new(PublishMessage)
	err := json.Unmarshal([]byte(message), publishMessage)
	if err != nil {
		return nil, err
	}
	return publishMessage, nil
}
func (rmq *RedisMQ) initFailedHandler() {
	rmq.recordOnce.Do(func() {
		rmq.recorder = failedlist.NewRecorder(rmq.recorderPersistencePath)
		rmq.recorder.Start()

		rmq.startHandleFailedMessage()
	})
}
func (rmq *RedisMQ) startHandleFailedMessage() {
	if rmq.recorder == nil {
		return
	}
	go func() {
		for {
			time.Sleep(time.Minute * 5)
			record := rmq.recorder.PickRecord()

			newFailedList := make([]*failedlist.Record, 0)
			for record != nil {
				ctx, _ := record.Ctx.EmbedIntoContext(context.Background())
				err := rmq.Publish(ctx, record.Topic, record.Message)
				if err != nil {
					//save failed record
					newFailedList = append(newFailedList, record)
				}
				record = rmq.recorder.PickRecord()
			}

			//save failed in recorder
			if len(newFailedList) > 0 {
				rmq.recorder.AddRecordList(newFailedList)
			}
		}
	}()
}
func (rmq *RedisMQ) PendingMessage(ctx context.Context, topic string) ([]string, error) {
	return nil, nil
}
func (rmq *RedisMQ) Publish(ctx context.Context, topic string, message string) error {
	publishMessage, err := marshalPublishMessage(ctx, message)
	if err != nil {
		return err
	}
	return drive.GetRedis().Publish(topic, publishMessage).Err()
}
func (rmq *RedisMQ) SubscribeWithReconnect(topic string, handler func(ctx context.Context, message string) error) int {
	sub := drive.GetRedis().Subscribe(topic)

	rmq.initFailedHandler()

	go func() {
		for {
			//msg := <- sub.Channel()
			msg, err := sub.ReceiveMessage()
			if err != nil {
				fmt.Println("Receive message failed, error:", err)
				continue
			}
			go func() {
				publishMessage, err := unmarshalPublishMessage(msg.Payload)
				if err != nil {
					fmt.Println("Unmarshal message failed, error:", err)
					return
				}
				ctx, _ := publishMessage.BadaCtx.EmbedIntoContext(context.Background())

				err = handler(ctx, publishMessage.Message)
				//若该消息未处理，则重新发送
				if err != nil {
					fmt.Println("Handle message with error: ", err)
					time.Sleep(requeue_delay)
					rmq.Publish(context.Background(), topic, publishMessage.Message)
				}
			}()
		}
	}()

	rmq.lock.Lock()
	defer rmq.lock.Unlock()
	id := rmq.curId
	rmq.curId++
	rmq.subHandler[rmq.curId] = sub
	return id
}

func (rmq *RedisMQ) Subscribe(topic string, handler func(ctx context.Context, message string)) int {
	sub := drive.GetRedis().Subscribe(topic)
	go func() {
		for {
			msg, err := sub.ReceiveMessage()
			if err != nil {
				fmt.Println("Receive message failed, error:", err)
				continue
			}
			go func() {
				publishMessage, err := unmarshalPublishMessage(msg.Payload)
				if err != nil {
					fmt.Println("Unmarshal message failed, error:", err)
					return
				}
				ctx, _ := publishMessage.BadaCtx.EmbedIntoContext(context.Background())

				handler(ctx, publishMessage.Message)
			}()
		}
	}()

	rmq.lock.Lock()
	defer rmq.lock.Unlock()
	id := rmq.curId
	rmq.curId++
	rmq.subHandler[rmq.curId] = sub
	return id
}

func (rmq *RedisMQ) Unsubscribe(hid int) {
	rmq.lock.Lock()
	defer rmq.lock.Unlock()
	sub, ok := rmq.subHandler[hid]
	if ok {
		sub.Close()
	}
}

func NewRedisMQ(recorderPersistencePath string) *RedisMQ {
	return &RedisMQ{
		curId:                   1,
		subHandler:              make(map[int]*redis.PubSub),
		recorderPersistencePath: recorderPersistencePath,
	}
}
