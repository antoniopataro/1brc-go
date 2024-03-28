package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type MeasurementToSave struct {
	count        uint32
	maxDecimal   int16
	minDecimal   int16
	totalDecimal int64
}

type MeasurementToStdout struct {
	averageDecimal int16
	maxDecimal     int16
	minDecimal     int16
	station        string
}

func (m MeasurementToStdout) Stdout() string {
	return fmt.Sprintf("%s=%.1f/%.1f/%.1f", m.station, float32(m.minDecimal/10), float32(m.averageDecimal/10), float32(m.maxDecimal/10))
}

type MeasurementToStream struct {
	TemperatureDecimal int16
}

const (
	chunkSize = 64 * 1024 * 1024
	fileName  = "measurements.txt"
)

func main() {
	file, err := os.OpenFile(fileName, os.O_RDONLY, 0444)
	if err != nil {
		log.Fatalf("error reading %s: %s", fileName, err)
	}
	defer file.Close()

	chunkStream := make(chan []byte)
	measurementsStream := make(chan map[string]MeasurementToStream, 16)
	wg := new(sync.WaitGroup)

	for i := 0; i <= runtime.NumCPU()-1; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for streamedChunk := range chunkStream {
				lines := bytes.Split(streamedChunk, []byte{'\n'})

				measurementsToStream := make(map[string]MeasurementToStream, len(lines))

				for _, line := range lines {
					if len(line) == 0 {
						continue
					}

					ss := strings.Split(string(line), ";")

					stationStr, temperatureStr := ss[0], ss[1]

					temperatureDecimal, err := strconv.ParseInt(temperatureStr[0:len(temperatureStr)-2]+temperatureStr[len(temperatureStr)-1:], 10, 16)
					if err != nil {
						os.WriteFile("error.txt", streamedChunk, 0644)

						log.Fatalf("error parsing the temperature for %s: %s", stationStr, err)
					}

					measurementsToStream[stationStr] = MeasurementToStream{
						TemperatureDecimal: int16(temperatureDecimal),
					}
				}

				measurementsStream <- measurementsToStream
			}
		}()
	}

	go func() {
		buffer := make([]byte, 0, chunkSize)
		leftover := make([]byte, 0)
		reader := bufio.NewReader(file)
		for i := 1; ; i++ {
			n, err := reader.Read(buffer[:cap(buffer)])
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}

				log.Fatalf("error reading chunk: %s", err)
			}

			chunk := buffer[:n]

			toStream := make([]byte, n)
			copy(toStream, chunk)

			lastNewLineIndex := bytes.LastIndex(chunk, []byte{'\n'})

			toStream = append(leftover, chunk[:lastNewLineIndex+1]...)
			leftover = make([]byte, len(chunk[lastNewLineIndex+1:]))
			copy(leftover, chunk[lastNewLineIndex+1:])

			chunkStream <- toStream
		}
		close(chunkStream)
		wg.Wait()
		close(measurementsStream)
	}()

	measurements := make(map[string]MeasurementToSave)

	for streamedMeasurements := range measurementsStream {
		for stationStr, measurement := range streamedMeasurements {
			temperatureDecimal := measurement.TemperatureDecimal

			measurement := MeasurementToSave{
				maxDecimal:   int16(temperatureDecimal),
				minDecimal:   int16(temperatureDecimal),
				totalDecimal: int64(temperatureDecimal),
			}

			if value, ok := measurements[stationStr]; ok {
				measurement = MeasurementToSave{
					count:        value.count + 1,
					maxDecimal:   value.maxDecimal,
					minDecimal:   value.minDecimal,
					totalDecimal: value.totalDecimal + int64(temperatureDecimal),
				}

				if int16(temperatureDecimal) > value.maxDecimal {
					measurement.maxDecimal = int16(temperatureDecimal)
				}

				if int16(temperatureDecimal) < value.minDecimal {
					measurement.minDecimal = int16(temperatureDecimal)
				}
			}

			measurements[stationStr] = measurement
		}
	}

	wg.Wait()

	measurementsSorted := make([]MeasurementToStdout, len(measurements))
	var i int16
	for station, measurement := range measurements {
		measurementsSorted[i] = MeasurementToStdout{
			averageDecimal: int16(measurement.totalDecimal / int64(measurement.count)),
			maxDecimal:     measurement.maxDecimal,
			minDecimal:     measurement.minDecimal,
			station:        station,
		}

		i++
	}

	sort.Slice(measurementsSorted, func(i, j int) bool {
		return measurementsSorted[i].station < measurementsSorted[j].station
	})

	var j int16
	result := "{"

	for _, measurement := range measurementsSorted {
		if j > 0 {
			result += ", "
		}

		j++
		result += measurement.Stdout()
	}

	result += "}"

	os.Stdout.WriteString(result)
}
