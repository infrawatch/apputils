package system

import (
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"strconv"
	"strings"
)

// Modifiable constants
var (
	ProcLimitColumns = 4
	ProcLimitPathFmt = "/proc/%d/limits"
	splitRex         = regexp.MustCompile("  +")
)

// GetProcLimits returns limits for the given process. Use -1 for actual process.
func GetProcLimits(PID int) (map[string]map[string]interface{}, error) {
	if PID == -1 {
		PID = os.Getpid()
	}

	data, err := ioutil.ReadFile(fmt.Sprintf(ProcLimitPathFmt, PID))
	if err != nil {
		return nil, err
	}

	indexes := []string{}
	out := make(map[string]map[string]interface{})
	for i, line := range strings.Split(string(data), "\n") {
		parts := splitRex.Split(line, ProcLimitColumns)
		if i == 0 {
			indexes = parts
			continue
		}

		value := make(map[string]interface{})
		for i, idx := range indexes {
			if i == 0 {
				continue
			}
			if len(parts) > i {
				if val, err := strconv.Atoi(parts[i]); err == nil {
					value[idx] = val
				} else {
					value[idx] = parts[i]
				}
			} else {
				value[idx] = ""
			}
		}
		out[parts[0]] = value
	}

	return out, nil
}
