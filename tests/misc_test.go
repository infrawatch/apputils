package tests

import (
	"fmt"
	"testing"

	"github.com/infrawatch/apputils/misc"
	"github.com/stretchr/testify/assert"
)

type TestStringer struct {
	numvalue int
	strvalue string
}

func (ts TestStringer) String() string {
	return fmt.Sprintf("%s %d!", ts.strvalue, ts.numvalue)
}

func TestStructsOperations(t *testing.T) {
	map1 := map[string]interface{}{
		"map": map[string]interface{}{
			"a": "A",
			"b": "B",
			"c": map[string]interface{}{
				"1": 1,
				"2": 2,
			},
			"d": []interface{}{"d1", "d2"},
		},
		"slice": []interface{}{
			"A",
			"B",
		},
		"float32":  float32(.32),
		"float64":  float64(.64),
		"int":      int(1),
		"int8":     int8(2),
		"int16":    int16(3),
		"int32":    int32(4),
		"int64":    int64(5),
		"bool":     true,
		"string":   "dub dub",
		"stringer": TestStringer{numvalue: 666, strvalue: "wubba lubba"},
	}
	map2 := map[string]string{"key": "value"}
	map3 := map[string]interface{}{
		"map":   map[string]interface{}{"foo": "bar"},
		"slice": []interface{}{"wubba", "lubba", "dub dub"},
	}

	t.Run("Test AssimilateMap", func(t *testing.T) {
		testAssimilate := map[string]string{
			"key":      "value",
			"a":        "A",
			"b":        "B",
			"1":        "1",
			"2":        "2",
			"d":        "d1,d2",
			"slice":    "A,B",
			"float32":  "0.320000",
			"float64":  "0.640000",
			"int":      "1",
			"int8":     "2",
			"int16":    "3",
			"int32":    "4",
			"int64":    "5",
			"bool":     "true",
			"string":   "dub dub",
			"stringer": "wubba lubba 666!",
		}
		misc.AssimilateMap(map1, &map2)
		assert.Equal(t, testAssimilate, map2)
	})

	t.Run("Test MergeMaps", func(t *testing.T) {
		testMerge := map[string]interface{}{
			"float32":  float32(.32),
			"float64":  float64(.64),
			"int":      int(1),
			"int8":     int8(2),
			"int16":    int16(3),
			"int32":    int32(4),
			"int64":    int64(5),
			"bool":     true,
			"string":   "dub dub",
			"stringer": TestStringer{numvalue: 666, strvalue: "wubba lubba"},
			"map":      map[string]interface{}{"foo": "bar"},
			"slice":    []interface{}{"wubba", "lubba", "dub dub"},
		}
		map4 := misc.MergeMaps(map1, map3)
		assert.Equal(t, testMerge, map4)
	})

}
