package task

import (
	"time"

	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
	"go.uber.org/atomic"
)

type ScheduleTaskWorker interface {
	Work() error
}

type ScheduleTaskConfig struct {
	FirstRun         string `toml:"first_run"`
	FirstDelaySecond int64  `toml:"first_delay_second"`
	IntervalSecond   int64  `toml:"interval_second"`
}

func NewScheduleTask(name string, config ScheduleTaskConfig, worker ScheduleTaskWorker) Task {
	return &ScheduleTask{
		name:       name,
		config:     &config,
		worker:     worker,
		stopSignal: make(chan int),
		isStop:     atomic.NewBool(false),
	}
}

type ScheduleTask struct {
	name       string
	config     *ScheduleTaskConfig
	worker     ScheduleTaskWorker
	stopSignal chan int
	isStop     *atomic.Bool
}

func (task *ScheduleTask) Start() error {
	if task.config.IntervalSecond == 0 {
		return errors.New("schedule task must have interval time config.")
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
		for d < 0 {
			start = start.Add(24 * time.Hour)

			d = start.Unix() - now.Unix()
		}

		startTime = time.Duration(d) * time.Second

	} else {
		startTime = time.Duration(task.config.FirstDelaySecond) * time.Second
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Errorf("schedule task: %s | stop with panic: %v", r)
			}

			task.isStop.Store(true)
		}()

		timer := time.NewTimer(startTime)

		for {
			select {
			case <-timer.C:
				err := task.worker.Work()
				if err != nil {
					log.Errorf("schedule task: %s | worker return error: %s", errors.Details(err))
				}
			case <-task.stopSignal:
			}

			timer.Reset(interval)
		}
	}()

	return nil
}

func (task *ScheduleTask) Stop(waitTime time.Duration) {
	log.Infof("schedule task %s | stopping...", task.name)

	if task.isStop.Load() {
		log.Infof("task %s | stopped")
	}

	timer := time.NewTimer(waitTime)
	select {
	case task.stopSignal <- 1:
		log.Infof("schedule task %s | stopped", task.name)
	case <-timer.C:
		log.Infof("schedule task %s | force stopped", task.name)
	}
}
