package scheduler

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/infrawatch/apputils/logging"
	"github.com/infrawatch/apputils/misc"
)

type taskState int
type schedulerState int

const (
	taskNew taskState = iota
	taskScheduled
	taskCancelled
	taskRemoved
)

const (
	scheduleStarted schedulerState = iota
	scheduleStopping
	scheduleStopped
)

type taskExec func(context.Context, *logging.Logger) (interface{}, error)

type task struct {
	interval time.Duration
	execute  taskExec
	timeout  int
	ticker   *time.Ticker
	state    taskState
}

type job struct {
	taskName    string
	dynamicCase reflect.SelectCase
}

//Result holds information about task run result
type Result struct {
	Task   string
	Output interface{}
	Error  error
}

//Scheduler holds tasks for scheduling
type Scheduler struct {
	tasks  map[string]*task
	jobs   []job
	log    *logging.Logger
	ctx    context.Context
	wg     *sync.WaitGroup
	cancel func()
	state  schedulerState
}

// New ... constructor
func New(logger *logging.Logger) (*Scheduler, error) {
	ctx, cancel := context.WithCancel(context.Background())
	scheduler := Scheduler{
		tasks:  make(map[string]*task),
		log:    logger,
		ctx:    ctx,
		cancel: cancel,
		jobs:   []job{},
		state:  scheduleStopped,
	}
	return &scheduler, nil
}

// RegisterTask registers task to scheduler for execution. It does not execute the task
func (sched *Scheduler) RegisterTask(name string, interval string, timeout int, execute taskExec) error {
	if _, ok := sched.tasks[name]; ok {
		return fmt.Errorf("task with given name already exists: %s", name)
	}

	if duration, err := misc.IntervalToDuration(interval); err == nil {
		sched.tasks[name] = &task{
			interval: duration,
			timeout:  timeout,
			execute:  execute,
			ticker:   nil,
			state:    taskNew,
		}
	} else {
		return err
	}

	return nil
}

//CancelTask removes task from schedule and stops appropriate job execution
func (sched *Scheduler) CancelTask(name string, remove bool) error {
	task, ok := sched.tasks[name]
	if !ok {
		return fmt.Errorf("task with given name (%s) does not exist", name)
	}
	if remove {
		if sched.state == scheduleStopped {
			delete(sched.tasks, name)
		} else {
			task.state = taskRemoved
		}
	} else {
		task.state = taskCancelled
	}
	return nil
}

//GetSchedule returns intervals for all tasks
func (sched *Scheduler) GetSchedule() map[string]string {
	output := map[string]string{}
	for task, data := range sched.tasks {
		output[task] = data.interval.String()
	}
	return output
}

//ExecuteTask runs task with given name
func (sched *Scheduler) executeTask(taskName string, outchan chan Result) {
	defer sched.wg.Done()

	if _, ok := sched.tasks[taskName]; !ok {
		sched.log.Metadata(logging.Metadata{"task": taskName})
		sched.log.Warn("requested execution of canceled task")
		return
	}

	//TODO: timeout
	result, err := (sched.tasks[taskName]).execute(sched.ctx, sched.log)
	if err != nil {
		sched.log.Metadata(logging.Metadata{"task": taskName, "error": err})
		sched.log.Warn("task execution failed")
	}
	outchan <- Result{
		Task:   taskName,
		Output: result,
		Error:  err,
	}
}

func (sched *Scheduler) cancelJob(index int) {
	copy(sched.jobs[index:], sched.jobs[index+1:])
	sched.jobs[len(sched.jobs)-1] = job{}
	sched.jobs = sched.jobs[:len(sched.jobs)-1]
}

func (sched *Scheduler) selectTask(outchan chan Result) {
	for {
		if len(sched.jobs) == 0 || sched.state == scheduleStopped {
			goto done
		}

		cases := make([]reflect.SelectCase, 0, len(sched.jobs))
		for _, job := range sched.jobs {
			cases = append(cases, job.dynamicCase)
		}

		index, ts, ok := reflect.Select(cases)
		name := (sched.jobs[index]).taskName

		if ok {
			// execute task
			sched.log.Metadata(logging.Metadata{"task": name, "timestamp": ts})
			task := sched.tasks[name]
			switch sched.tasks[name].state {
			case taskScheduled:
				sched.log.Debug("executing")
				sched.wg.Add(1)
				go sched.executeTask(name, outchan)
			case taskCancelled:
				sched.log.Debug("cancelling task")
				sched.cancelJob(index)
				task.ticker.Stop()
			case taskRemoved:
				sched.log.Debug("deleting task")
				sched.cancelJob(index)
				task.ticker.Stop()
				delete(sched.tasks, name)
			}
		} else {
			// task timer channel is closed, so task has been canceled
			sched.log.Metadata(logging.Metadata{"task": name})
			sched.log.Debug("deleting task's job due to timer channel close")
			sched.cancelJob(index)
		}
	}
done:
	sched.state = scheduleStopped
	sched.jobs = make([]job, 0, len(sched.tasks))
	close(outchan)
	sched.log.Debug("scheduler stopped")
}

//Start schedules tickers to each task. Returns Results channel
func (sched *Scheduler) Start(bufferSize int, block bool) chan Result {
	sched.state = scheduleStarted
	// dynamically create select cases for all tasks -> create jobs
	sched.jobs = make([]job, 0, len(sched.tasks))

	for name := range sched.tasks {
		(sched.tasks[name]).state = taskScheduled
		(sched.tasks[name]).ticker = time.NewTicker(sched.tasks[name].interval)
		sched.jobs = append(sched.jobs, job{
			taskName: name,
			dynamicCase: reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf((sched.tasks[name]).ticker.C),
			},
		})

		sched.log.Metadata(logging.Metadata{"task": name, "timer": sched.tasks[name].interval.String()})
		sched.log.Debug("started timer for task")
	}

	sched.wg = new(sync.WaitGroup)
	outchan := make(chan Result, bufferSize)
	// start tasks execution loop based on tickers
	go sched.selectTask(outchan)
	sched.log.Debug("scheduler loop started")
	if block {
		sched.Wait()
	}
	return outchan
}

//Stop execution of the whole schedule
func (sched *Scheduler) Stop(removeTasks bool) {
	sched.state = scheduleStopping
	jobs := make([]string, 0, len(sched.jobs))
	for _, jb := range sched.jobs {
		jobs = append(jobs, jb.taskName)
	}
	for _, task := range jobs {
		sched.CancelTask(task, removeTasks)
	}
	sched.cancel()
	sched.Wait()
	sched.state = scheduleStopped
}

//Wait for scheduler finishing started jobs.
func (sched *Scheduler) Wait() {
	for len(sched.jobs) != 0 {
		time.Sleep(1)
	}
	if sched.wg != nil {
		sched.wg.Wait()
	}
}
