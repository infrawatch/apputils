package tests

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"testing"

	"github.com/infrawatch/apputils/logging"
	"github.com/infrawatch/apputils/scheduler"
	"github.com/stretchr/testify/assert"
)

func TestSchedulerBasicOperations(t *testing.T) {
	tmpdir, err := ioutil.TempDir(".", "scheduler_test")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpdir)
	logpath := path.Join(tmpdir, "test.log")
	log, err := logging.NewLogger(logging.DEBUG, logpath)
	if err != nil {
		fmt.Printf("Failed to open log file %s.\n", logpath)
		os.Exit(2)
	}
	defer log.Destroy()

	sched, _ := scheduler.New(log)

	t.Run("Test registering tasks", func(t *testing.T) {
		sched.RegisterTask("test1", "1s", 0, func(ctx context.Context, log *logging.Logger) (interface{}, error) {
			return "test1", nil
		})
		sched.RegisterTask("test2", "1m", 0, func(ctx context.Context, log *logging.Logger) (interface{}, error) {
			return nil, nil
		})
		sched.RegisterTask("test3", "1h", 0, func(ctx context.Context, log *logging.Logger) (interface{}, error) {
			return nil, nil
		})
		sched.RegisterTask("test4", "1d", 0, func(ctx context.Context, log *logging.Logger) (interface{}, error) {
			return nil, nil
		})
		schedule := sched.GetSchedule()
		assert.Equal(t, 4, len(schedule))
		assert.Equal(t, "1s", schedule["test1"])
		assert.Equal(t, "1m0s", schedule["test2"])
		assert.Equal(t, "1h0m0s", schedule["test3"])
		assert.Equal(t, "24h0m0s", schedule["test4"])
	})

	t.Run("Fail task overwrite", func(t *testing.T) {
		err := sched.RegisterTask("test1", "2s", 0, func(ctx context.Context, log *logging.Logger) (interface{}, error) {
			return nil, nil
		})
		assert.Error(t, err)
	})

	t.Run("Test task cancellation before schedule execution", func(t *testing.T) {
		sched.CancelTask("test2", true)
		sched.CancelTask("test3", true)
		sched.CancelTask("test4", true)
		assert.Equal(t, 1, len(sched.GetSchedule()))
	})

	t.Run("Test schedule execution and blocking stop", func(t *testing.T) {
		sched.RegisterTask("test1.a", "2s", 0, func(ctx context.Context, log *logging.Logger) (interface{}, error) {
			return "test1.a", nil
		})
		sched.RegisterTask("test1.b", "3s", 0, func(ctx context.Context, log *logging.Logger) (interface{}, error) {
			return "test1.b", nil
		})
		assert.Equal(t, 3, len(sched.GetSchedule()))

		// Buffer length is 1 so there will always be only one task output at channel read time.
		// Since intervals of all tasks are multiples of 1 second, we can surely tell order of outputs
		// There can be only test1 task output in first second, test1 or test1.a output in second second
		// depending on which task will submit it's output first and at last test1 or test1.b output
		output := sched.Start(1, false)
		for i := range []int{0, 1, 2, 3, 4} {
			result := <-output
			switch i {
			case 0:
				assert.Equal(t, "test1", result.Output.(string))
			case 1:
				expected := result.Output.(string) == "test1" || result.Output.(string) == "test1.a"
				assert.Equal(t, true, expected)
			case 2:
				expected := result.Output.(string) == "test1" || result.Output.(string) == "test1.a"
				assert.Equal(t, true, expected)
			case 3:
				expected := result.Output.(string) == "test1" || result.Output.(string) == "test1.b"
				assert.Equal(t, true, expected)
			case 4:
				expected := result.Output.(string) == "test1" || result.Output.(string) == "test1.b"
				assert.Equal(t, true, expected)
			}
		}
		sched.Stop(true)
		assert.Equal(t, 0, len(sched.GetSchedule()))
	})

	t.Run("Test task cancellation during schedule execution", func(t *testing.T) {
		sched.Wait()

		sched.RegisterTask("test2", "1s", 0, func(ctx context.Context, log *logging.Logger) (interface{}, error) {
			return "test2", nil
		})
		sched.RegisterTask("test2.a", "2s", 0, func(ctx context.Context, log *logging.Logger) (interface{}, error) {
			return "test2.a", nil
		})
		sched.RegisterTask("test2.b", "3s", 0, func(ctx context.Context, log *logging.Logger) (interface{}, error) {
			return "test2.b", nil
		})
		assert.Equal(t, 3, len(sched.GetSchedule()))

		output := sched.Start(1, false)
		for i := range []int{0, 1, 2, 3, 4, 5} {
			result := <-output
			switch i {
			case 0:
				assert.Equal(t, "test2", result.Output.(string))
				sched.CancelTask("test2.b", true)
			case 1:
				expected := result.Output.(string) == "test2" || result.Output.(string) == "test2.a"
				assert.Equal(t, true, expected)
			case 2:
				expected := result.Output.(string) == "test2" || result.Output.(string) == "test2.a"
				assert.Equal(t, true, expected)
			case 3:
				assert.Equal(t, "test2", result.Output.(string))
			case 4:
				expected := result.Output.(string) == "test2" || result.Output.(string) == "test2.a"
				assert.Equal(t, true, expected)
			case 5:
				expected := result.Output.(string) == "test2" || result.Output.(string) == "test2.a"
				assert.Equal(t, true, expected)
			}
		}
		sched.Stop(false)
		assert.Equal(t, 2, len(sched.GetSchedule()))
	})

	// output log file content
	if file, err := os.Open(logpath); err == nil {
		b, _ := ioutil.ReadAll(file)

		fmt.Printf("----------LOG----------\n%s\n-----------------------\n", string(b))
	}
}
