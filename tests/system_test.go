package tests

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/infrawatch/apputils/system"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const procLimitData = `Limit                     Soft Limit           Hard Limit           Units
Max cpu time              unlimited            unlimited            seconds
Max file size             unlimited            unlimited            bytes
Max data size             unlimited            unlimited            bytes
Max stack size            8388608              unlimited            bytes
Max core file size        unlimited            unlimited            bytes
Max resident set          unlimited            unlimited            bytes
Max processes             125751               125751               processes
Max open files            1024                 1048576              files
Max locked memory         65536                65536                bytes
Max address space         unlimited            unlimited            bytes
Max file locks            unlimited            unlimited            locks
Max pending signals       125751               125751               signals
Max msgqueue size         819200               819200               bytes
Max nice priority         0                    0
Max realtime priority     0                    0
Max realtime timeout      unlimited            unlimited            us`

func TestProcLimits(t *testing.T) {
	tmpdir, err := ioutil.TempDir(".", "system_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpdir)

	// save test content
	file, err := os.Create(path.Join(tmpdir, "0_limits"))
	require.NoError(t, err)
	file.WriteString(procLimitData)
	require.NoError(t, file.Close())

	t.Run("Test parsed limit file", func(t *testing.T) {
		system.ProcLimitPathFmt = path.Join(tmpdir, "%d_limits")

		expected := map[string]map[string]interface{}{
			"Max cpu time": {
				"Soft Limit": "unlimited",
				"Hard Limit": "unlimited",
				"Units":      "seconds",
			},
			"Max file size": {
				"Soft Limit": "unlimited",
				"Hard Limit": "unlimited",
				"Units":      "bytes",
			},
			"Max data size": {
				"Soft Limit": "unlimited",
				"Hard Limit": "unlimited",
				"Units":      "bytes",
			},
			"Max stack size": {
				"Soft Limit": 8388608,
				"Hard Limit": "unlimited",
				"Units":      "bytes",
			},
			"Max core file size": {
				"Soft Limit": "unlimited",
				"Hard Limit": "unlimited",
				"Units":      "bytes",
			},
			"Max resident set": {
				"Soft Limit": "unlimited",
				"Hard Limit": "unlimited",
				"Units":      "bytes",
			},
			"Max processes": {
				"Soft Limit": 125751,
				"Hard Limit": 125751,
				"Units":      "processes",
			},
			"Max open files": {
				"Soft Limit": 1024,
				"Hard Limit": 1048576,
				"Units":      "files",
			},
			"Max locked memory": {
				"Soft Limit": 65536,
				"Hard Limit": 65536,
				"Units":      "bytes",
			},
			"Max address space": {
				"Soft Limit": "unlimited",
				"Hard Limit": "unlimited",
				"Units":      "bytes",
			},
			"Max file locks": {
				"Soft Limit": "unlimited",
				"Hard Limit": "unlimited",
				"Units":      "locks",
			},
			"Max pending signals": {
				"Soft Limit": 125751,
				"Hard Limit": 125751,
				"Units":      "signals",
			},
			"Max msgqueue size": {
				"Soft Limit": 819200,
				"Hard Limit": 819200,
				"Units":      "bytes",
			},
			"Max nice priority": {
				"Soft Limit": 0,
				"Hard Limit": 0,
				"Units":      "",
			},
			"Max realtime priority": {
				"Soft Limit": 0,
				"Hard Limit": 0,
				"Units":      "",
			},
			"Max realtime timeout": {
				"Soft Limit": "unlimited",
				"Hard Limit": "unlimited",
				"Units":      "us",
			},
		}

		parsed, err := system.GetProcLimits(0)
		require.NoError(t, err)
		assert.Equal(t, expected, parsed, "Did not parse correctly")
	})

}
