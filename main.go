package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"slices"
	"strconv"
	"strings"
)

type Measurement struct {
	count        uint32
	station      string
	maxDecimal   int16
	minDecimal   int16
	totalDecimal int64
}

func main() {
	file, err := os.OpenFile("measurements.txt", os.O_RDONLY, 0444)
	if err != nil {
		log.Fatalf("error loading the file: %s", err)
	}

	measurements := make(map[string]Measurement)

	reader := bufio.NewReader(file)

	for l := 1; ; l++ {
		fmt.Printf("%d\r", l)

		bs, err := reader.ReadBytes('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			log.Fatalf("error reading line %d: %s", l, err)
		}

		ss := strings.Split(string(bs[:len(bs)-1]), ";")

		stationStr := ss[0]
		temperatureStr := ss[1]

		temperatureDecimal, err := strconv.ParseInt(temperatureStr[0:len(temperatureStr)-2]+temperatureStr[len(temperatureStr)-1:], 10, 16)
		if err != nil {
			log.Fatalf("error parsing the temperature: %s", err)
		}

		if value, ok := measurements[stationStr]; ok {
			measurement := Measurement{
				count:        value.count + 1,
				maxDecimal:   value.maxDecimal,
				minDecimal:   value.minDecimal,
				station:      stationStr,
				totalDecimal: value.totalDecimal + temperatureDecimal,
			}

			if int16(temperatureDecimal) > value.maxDecimal {
				measurement.maxDecimal = int16(temperatureDecimal)
			}

			if int16(temperatureDecimal) < value.minDecimal {
				measurement.minDecimal = int16(temperatureDecimal)
			}

			measurements[stationStr] = measurement

			continue
		}

		measurement := Measurement{
			count:        1,
			maxDecimal:   int16(temperatureDecimal),
			minDecimal:   int16(temperatureDecimal),
			station:      stationStr,
			totalDecimal: int64(temperatureDecimal),
		}

		measurements[stationStr] = measurement
	}

	keys := make([]string, 0, len(measurements))

	for key := range measurements {
		keys = append(keys, key)
	}

	slices.Sort(keys)

	sortedMeasurements := make([]Measurement, 0, len(measurements))

	for _, key := range keys {
		sortedMeasurements = append(sortedMeasurements, measurements[key])
	}

	var i int16
	result := "{"

	for _, measurement := range sortedMeasurements {
		if i > 0 {
			result += ", "
		}

		i++
		result += fmt.Sprintf("%s=%.1f/%.1f/%.1f", measurement.station, float32(measurement.minDecimal/10), float32(measurement.totalDecimal/int64(measurement.count)), float32(measurement.maxDecimal/10))
	}

	result += "}"

	os.Stdout.WriteString(fmt.Sprintf("%v\n", result))
}
