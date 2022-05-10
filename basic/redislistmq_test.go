package basic

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/KL-Engineering/imq/drive"
)

func TestGetPendingMessage(t *testing.T) {
	err := drive.OpenRedis("127.0.0.1", 6379, "")
	if err != nil {
		fmt.Println(err)
		return
	}

	mq := NewRedisListMQ("failed_list.json", 20)

	mq.PendingMessage(context.Background(), "kfps:attachment")
	time.Sleep(time.Second)
}
