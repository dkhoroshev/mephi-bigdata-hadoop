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
			"%d,%d,%s,%s",
			rand.Intn(maxX),
			rand.Intn(maxY),
			users[rand.Intn(len(users))],
			randomTimestamp(),
		)
	}

	return clicks
}

func randomTimestamp() time.Time {
	randomTime := rand.Int63n(time.Now().Unix()-94608000) + 94608000

	randomNow := time.Unix(randomTime, 0)

	return randomNow
}
