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

func (monitor *ConcurrentTask) Start() error {
	if monitor.config.IntervalSecond == 0 {
		return errors.New("concurrent monitor must have interval time config.")
	}

	now := time.Now()

	var startTime time.Duration
	interval := time.Duration(monitor.config.IntervalSecond) * time.Second

	if monitor.config.FirstRun != "" {

		start, err := time.Parse("2006-01-02 15:04:05 MST", monitor.config.FirstRun+" CST")
		if err != nil {
			return errors.Annotatef(err, "parse first_run fail")
		}

		d := start.Unix() - now.Unix()
		if d < 0 {
			d = 0
		}

		startTime = time.Duration(d) * time.Second

	} else {
		startTime = time.Duration(monitor.config.FirstDelaySecond) * time.Second
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Errorf("monitor: %s | stop with panic: %v", r)
			}

			monitor.isStop.Store(true)
		}()

		timer := time.NewTimer(startTime)

	workerLoop:
		for {
			timer.Reset(interval)

			select {
			case <-timer.C:
				records, err := monitor.worker.Grep()
				if err != nil {
					log.Errorf("monitor: %s | grep record return err: %s", monitor.name, errors.Details(err))
					continue workerLoop
				}

				if taskNum := len(records); taskNum > 0 {
					log.Infof("monitor: %s | grep record count: %d", monitor.name, taskNum)

					workNum := monitor.config.ConcurrentNum
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
									log.Errorf("monitor: %s | worker panic: %v", monitor.name, r)
								}

								waiter.Done()
							}()

							for {
								select {
								case record, ok := <-taskChan:
									if ok {
										err := monitor.worker.Work(record)
										if err != nil {
											rs, _ := json.Marshal(record)
											log.Errorf("monitor: %s | worker for record %s return err: %v", monitor.name, rs, errors.Details(err))
										}
									} else {
										return
									}
								case <-monitor.forceStopSignal:
									log.Infof("monitor: %s | worker force stop")
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

			case <-monitor.stopSignal:
				return
			}

		}

	}()

	return nil
}

func (monitor *ConcurrentTask) Stop(waitTime time.Duration) {
	log.Infof("monitor %s | stopping...", monitor.name)

	timer := time.NewTimer(waitTime)
	select {
	case monitor.stopSignal <- 1:
		log.Info("monitor %s | stopped", monitor.name)
	case <-timer.C:
		log.Info("monitor %s force stopping...", monitor.name)
		close(monitor.forceStopSignal)

		timer.Reset(force_stop_wait_second * time.Second)
		select {
		case monitor.stopSignal <- 1:
			log.Info("monitor %s | force stopped", monitor.name)
		case <-timer.C:
			log.Info("monitor %s | force stopped for timeout", monitor.name)
		}
	}

}
