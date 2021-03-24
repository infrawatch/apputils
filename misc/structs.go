package misc

import (
	"fmt"
	"log"
	"strings"
)

// AssimilateMap recursively saves content of the given map to destination map of strings
func AssimilateMap(theMap map[string]interface{}, destination *map[string]string) {
	defer func() { // recover from any panic
		if r := recover(); r != nil {
			log.Printf("Panic:recovered in assimilateMap %v\n", r)
		}
	}()
	for key, val := range theMap {
		switch value := val.(type) {
		case map[string]interface{}:
			// go one level deeper in the map
			AssimilateMap(value, destination)
		case []interface{}:
			// transform slice value to comma separated list and assimilate it
			aList := make([]string, 0, len(value))
			for _, item := range value {
				if itm, ok := item.(string); ok {
					aList = append(aList, itm)
				}
			}
			(*destination)[key] = strings.Join(aList, ",")
		case float64, float32:
			(*destination)[key] = fmt.Sprintf("%f", value)
		case int, int8, int16, int32, int64:
			(*destination)[key] = fmt.Sprintf("%d", value)
		case bool:
			(*destination)[key] = fmt.Sprintf("%t", value)
		default:
			// assimilate KV pair
			if stringer, ok := value.(fmt.Stringer); ok {
				(*destination)[key] = stringer.String()
			} else {
				(*destination)[key] = value.(string)
			}
		}
	}
}

// MergeMaps merges given maps into a new one
func MergeMaps(ms ...map[string]interface{}) map[string]interface{} {
	res := make(map[string]interface{})
	for _, m := range ms {
		for k, v := range m {
			switch value := v.(type) {
			case map[string]interface{}:
				// go one level deeper in the map
				res[k] = MergeMaps(value)
			case []interface{}:
				vCopy := make([]interface{}, len(value))
				copy(vCopy, value)
				res[k] = vCopy
			default:
				// assimilate KV pair
				res[k] = value
			}
		}
	}
	return res
}
