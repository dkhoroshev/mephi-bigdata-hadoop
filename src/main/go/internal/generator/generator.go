package generator

import (
	"fmt"
	"math/rand"
	"time"
)

var countRecords int

func init() {
	countRecords = 300000
}

func GenerateClicks(maxX int, maxY int, users []string) []string {

	clicks := make([]string, countRecords)

	for i := 0; i < countRecords; i++ {
		clicks[i] = fmt.Sprintf(
			"%d,%d,%s,%d",
			rand.Intn(maxX),
			rand.Intn(maxY),
			users[rand.Intn(len(users))],
			randomTimestamp(),
		)
	}

	return clicks
}

func GenerateArea(screenX int, screenY int, countX int, countY int) []string {

	area := make([]string, countX*countY)

	partX := screenX / countX
	partY := screenY / countY

	var minX, maxX, minY, maxY int

	counter := 0

	for i := 0; i < countX; i++ {
		minX = partX * i
		maxX = partX * (i + 1)
		for j := 0; j < countY; j++ {
			minY = partY * j
			maxY = partY * (j + 1)
			counter = counter + 1
			area = append(area, fmt.Sprintf(
				"%d,%d,%d,%d-%s",
				minX,
				minY,
				maxX,
				maxY,
				fmt.Sprintf("area_%d", counter),
			))
		}

	}

	return area
}

func GenerateTemperatureDirectory(t1 int, t2 int) []string {

	temperature := make([]string, 3)

	temperature[0] = fmt.Sprintf(
		"%d,%d - %s",
		0,
		t1,
		"Blue",
	)

	temperature[1] = fmt.Sprintf(
		"%d,%d - %s",
		t1+1,
		t2,
		"Yellow",
	)

	temperature[2] = fmt.Sprintf(
		"%d,%d - %s",
		t2+1,
		9999999,
		"Red",
	)

	return temperature
}

func randomTimestamp() int64 {
	return rand.Int63n(time.Now().Unix()-94608000) + 94608000
}
