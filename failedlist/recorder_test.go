package failedlist

import (
	"testing"
	"time"

	gintrace "github.com/KL-Engineering/gin-trace"
)

func TestRecorder(t *testing.T) {
	recorder := NewRecorder("./test.json")
	recorder.Start()

	recorder.AddRecord(&Record{
		Ctx: gintrace.BadaCtx{
			PrevTid:  "1",
			CurrTid:  "2",
			EntryTid: "3",
		},
		Time:    time.Now(),
		Topic:   "test123",
		Message: "aabbcc",
	})
	recorder.AddRecord(&Record{
		Time:    time.Now(),
		Topic:   "4",
		Message: "aabbccc",
	})
	recorder.AddRecord(&Record{
		Ctx: gintrace.BadaCtx{
			PrevTid:  "14",
			CurrTid:  "24",
			EntryTid: "34",
		},
		Time:    time.Now(),
		Topic:   "666",
		Message: "aabbccd",
	})

	time.Sleep(time.Second * 2)

	t.Logf("%#v", recorder.PickRecord())
	recorder.AddRecord(&Record{
		Time:    time.Now(),
		Topic:   "good",
		Message: "byte",
	})

	time.Sleep(time.Second * 5)
}
