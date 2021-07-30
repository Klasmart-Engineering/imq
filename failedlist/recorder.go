package failedlist

import (
	"encoding/json"
	"fmt"
	"gitlab.badanamu.com.cn/calmisland/common-cn/helper"
	"io"
	"io/ioutil"
	"os"
	"sync"
	"time"
)

const (
	saveDuration = time.Second * 5
)

type Record struct {
	Ctx helper.BadaCtx

	Time    time.Time
	Topic   string
	Message string
}

type Recorder struct {
	sync.Mutex
	list            []*Record
	persistencePath string
	persistence     *os.File
	quit            chan struct{}
}

func (r *Recorder) Stop() {
	r.persistence.Close()
	r.quit <- struct{}{}
}

func (r *Recorder) Start() {
	f, err := os.OpenFile(r.persistencePath, os.O_CREATE|os.O_RDWR, 0777)
	if err != nil {
		panic(err)
	}

	r.persistence = f

	go func() {
		for {
			select {
			case <-r.quit:
				return
			default:
				time.Sleep(saveDuration)
				err := r.saveRecords()
				if err != nil {
					fmt.Println("Save records failed, err:", err)
				}
			}

		}

	}()
}

func (r *Recorder) AddRecord(rd *Record) {
	r.Lock()
	defer r.Unlock()

	r.list = append(r.list, rd)
}

func (r *Recorder) AddRecordList(rds []*Record) {
	r.Lock()
	defer r.Unlock()

	r.list = append(r.list, rds...)
}

func (r *Recorder) PickRecord() *Record {
	r.Lock()
	defer r.Unlock()

	if len(r.list) > 0 {
		record := r.list[0]
		r.list = r.list[1:]
		return record
	}
	return nil
}

func (r *Recorder) loadRecords() {
	r.Lock()
	defer r.Unlock()

	data, err := ioutil.ReadFile(r.persistencePath)
	if err != nil {
		fmt.Println("Read file failed, err:", err)
		return
	}
	if len(data) < 1 {
		return
	}
	records := make([]*Record, 0)
	err = json.Unmarshal(data, &records)
	if err != nil {
		fmt.Println("File failed, err:", err)
		return
	}
	r.list = records
}

func (r *Recorder) saveRecords() error {
	r.persistence.Truncate(0)
	r.persistence.Seek(0, io.SeekStart)

	data, err := json.Marshal(r.list)
	if err != nil {
		return err
	}
	r.persistence.WriteString(string(data))
	return nil
}

func NewRecorder(path string) *Recorder {
	//read all records
	recorder := &Recorder{
		persistencePath: path,
		quit:            make(chan struct{}),
	}
	if recorder.persistencePath == "" {
		recorder.persistencePath = os.TempDir() + "/" + "imq_failed_persistence.json"
	}

	recorder.loadRecords()

	return recorder
}
