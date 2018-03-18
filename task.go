package task

import (
	"time"

	"encoding/json"
	"sync"

	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
	"go.uber.org/atomic"
)

type Task interface {
	Start() error
	Stop(waitTime time.Duration)
}

const (
	force_stop_wait_second = 5
)

func NewConcurrentTask(name string, config ConcurrentTaskConfig, worker ConcurrentTaskWorker) Task {
	return &ConcurrentTask{
		name:            name,
		config:          &config,
		worker:          worker,
		stopSignal:      make(chan int),
		forceStopSignal: make(chan int),
		isStop:          atomic.NewBool(false),
	}
}

type ConcurrentTaskWorker interface {
	Grep() ([]interface{}, error)
	Work(interface{}) error
}

type ConcurrentTaskConfig struct {
	FirstRun         string `toml:"first_run"`
	FirstDelaySecond int64  `toml:"first_delay_second"`
	IntervalSecond   int64  `toml:"interval_second"`
	ConcurrentNum    int    `toml:"concurrent_num"`
}

type ConcurrentTask struct {
	name            string
	config          *ConcurrentTaskConfig
	worker          ConcurrentTaskWorker
	stopSignal      chan int
	forceStopSignal chan int
	isStop          *atomic.Bool
}

func (task *ConcurrentTask) Start() error {
	if task.config.IntervalSecond == 0 {
		return errors.New("concurrent task must have interval time config.")
	}

	now := time.Now()

	var startTime time.Duration
	interval := time.Duration(task.config.IntervalSecond) * time.Second

	if task.config.FirstRun != "" {

		start, err := time.Parse("2006-01-02 15:04:05 MST", task.config.FirstRun+" CST")
		if err != nil {
			return errors.Annotatef(err, "parse first_run fail")
		}

		d := start.Unix() - now.Unix()
		if d < 0 {
			d = 0
		}

		startTime = time.Duration(d) * time.Second

	} else {
		startTime = time.Duration(task.config.FirstDelaySecond) * time.Second
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Errorf("task: %s | stop with panic: %v", r)
			}

			task.isStop.Store(true)
		}()

		timer := time.NewTimer(startTime)

	workerLoop:
		for {

			select {
			case <-timer.C:
				records, err := task.worker.Grep()
				if err != nil {
					log.Errorf("task: %s | grep record return err: %s", task.name, errors.Details(err))
					continue workerLoop
				}

				if taskNum := len(records); taskNum > 0 {
					log.Infof("task: %s | grep record count: %d", task.name, taskNum)

					workNum := task.config.ConcurrentNum
					if taskNum < workNum {
						workNum = taskNum
					}

					taskChan := make(chan interface{}, workNum)

					waiter := &sync.WaitGroup{}

					for ; workNum > 0; workNum-- {
						waiter.Add(1)
						go func() {
							defer func() {
								if r := recover(); r != nil {
									log.Errorf("task: %s | worker panic: %v", task.name, r)
								}

								waiter.Done()
							}()

							for {
								select {
								case record, ok := <-taskChan:
									if ok {
										err := task.worker.Work(record)
										if err != nil {
											rs, _ := json.Marshal(record)
											log.Errorf("task: %s | worker for record %s return err: %v", task.name, rs, errors.Details(err))
										}
									} else {
										return
									}
								case <-task.forceStopSignal:
									log.Infof("task: %s | worker force stop")
									return
								}
							}
						}()
					}

					for _, record := range records {
						taskChan <- record
					}
					close(taskChan)

					waiter.Wait()

				}

			case <-task.stopSignal:
				return
			}

			timer.Reset(interval)
		}

	}()

	return nil
}

func (task *ConcurrentTask) Stop(waitTime time.Duration) {
	log.Infof("task %s | stopping...", task.name)

	if task.isStop.Load() {
		log.Infof("task %s | stopped")
	}

	timer := time.NewTimer(waitTime)
	select {
	case task.stopSignal <- 1:
		log.Infof("task %s | stopped", task.name)
	case <-timer.C:
		log.Infof("task %s | force stopping...", task.name)
		close(task.forceStopSignal)

		if task.isStop.Load() {
			log.Infof("task %s | force stopped")
		}

		timer.Reset(force_stop_wait_second * time.Second)
		select {
		case task.stopSignal <- 1:
			log.Infof("task %s | force stopped", task.name)
		case <-timer.C:
			log.Infof("task %s | force stopped for timeout", task.name)
		}
	}

}
