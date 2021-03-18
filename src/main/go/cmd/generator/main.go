package main

import (
	"flag"
	"go/internal/generator"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
)

var resolutionScreen string
var countX string
var countY string
var temperature string
var arrayUsers []string

func init() {
	flag.StringVar(&resolutionScreen, "r", "1280x1024", "Resolution screen, example: 1280x1024")
	flag.StringVar(&countX, "x", "2", "Parts for X, example: 2")
	flag.StringVar(&countY, "y", "2", "Parts for Y, example: 3")
	flag.StringVar(&temperature, "t", "2000,21000", "Temperature, example: 100,200")
	arrayUsers = append(arrayUsers,
		"user1",
		"user2",
		"user3")
}

func main() {
	flag.Parse()
	log.Println(resolutionScreen)
	log.Println(countX)
	log.Println(countY)
	log.Println(temperature)
	log.Println(arrayUsers)

	splitString := strings.Split(resolutionScreen, "x")
	x, y := splitString[0], splitString[1]

	intX, _ := strconv.ParseInt(x, 10, 32)
	intY, _ := strconv.ParseInt(y, 10, 32)

	stringArray := generator.GenerateClicks(int(intX), int(intY), arrayUsers)

	stringText := strings.Join(stringArray, "\n")

	err := ioutil.WriteFile("file.txt", []byte(stringText), 0644)
	if err != nil {
		log.Println(err)
	}

	log.Println(stringArray)

}
