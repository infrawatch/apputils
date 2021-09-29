package misc

import (
	"fmt"
	"regexp"
	"strconv"
	"time"
)

var (
	intervalRegex = regexp.MustCompile(`(\d*)([smhd])`)
)

// IntervalToDuration converts interval string to equivalent duration
func IntervalToDuration(interval string) (time.Duration, error) {
	var out time.Duration

	if match := intervalRegex.FindStringSubmatch(interval); match != nil {
		var units time.Duration
		switch match[2] {
		case "s":
			units = time.Second
		case "m":
			units = time.Minute
		case "h":
			units = time.Hour
		case "d":
			units = time.Hour * 24
		default:
			return out, fmt.Errorf("invalid interval units (%s)", match[2])
		}
		num, err := strconv.Atoi(match[1])
		if err != nil {
			return out, fmt.Errorf("invalid interval value (%s): %s", match[3], err)
		}
		out = time.Duration(int64(num) * int64(units))
	} else {
		return out, fmt.Errorf("invalid interval value (%s)", interval)
	}

	return out, nil
}
