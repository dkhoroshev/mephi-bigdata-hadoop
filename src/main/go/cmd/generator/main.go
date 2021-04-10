package main

import (
	"flag"
	//"go/internal/generator"
	"generator/internal/generator"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
)

var (
	resolutionScreen string
	countX           string
	countY           string
	temperature      string
	arrayUsers       []string
)

func init() {
	flag.StringVar(&resolutionScreen, "r", "", "Resolution screen, example: 1280x1024")
	flag.StringVar(&countX, "x", "", "Parts for X, example: 2")
	flag.StringVar(&countY, "y", "", "Parts for Y, example: 3")
	flag.StringVar(&temperature, "t", "", "Temperature, example: 100,200")
	arrayUsers = append(arrayUsers,
		"user1",
		"user2",
		"user3")
}

func main() {
	flag.Parse()
	// Определяем расширение экрана
	splitString := strings.Split(resolutionScreen, "x")
	x, y := splitString[0], splitString[1]

	intX, _ := strconv.ParseInt(x, 10, 32)
	intY, _ := strconv.ParseInt(y, 10, 32)

	clicksArray := generator.GenerateClicks(int(intX), int(intY), arrayUsers)

	clicksText := strings.Join(clicksArray, "\n")

	// Генерируем файлы с кликами
	errSaveClicks := ioutil.WriteFile("clicks1.txt", []byte(clicksText), 0644)
	if errSaveClicks != nil {
		log.Println(errSaveClicks)
	}
	errSaveClicks = ioutil.WriteFile("clicks2.txt", []byte(clicksText), 0644)
	if errSaveClicks != nil {
		log.Println(errSaveClicks)
	}
	errSaveClicks = ioutil.WriteFile("clicks3.txt", []byte(clicksText), 0644)
	if errSaveClicks != nil {
		log.Println(errSaveClicks)
	}

	// Определяем параметры для построения словарей
	// справочник областей экрана
	countX, _ := strconv.ParseInt(countX, 10, 32)
	countY, _ := strconv.ParseInt(countY, 10, 32)

	areaArray := generator.GenerateArea(int(intX), int(intY), int(countX), int(countY))

	areaText := strings.Join(areaArray, "\n")

	errScreenArea := ioutil.WriteFile("screen_area.txt", []byte(areaText), 0644)
	if errScreenArea != nil {
		log.Println(errScreenArea)
	}

	// справочник температур
	splitString = strings.Split(temperature, ",")
	t1, t2 := splitString[0], splitString[1]

	temp1, _ := strconv.ParseInt(t1, 10, 32)
	temp2, _ := strconv.ParseInt(t2, 10, 32)

	temperatureDirectoryArray := generator.GenerateTemperatureDirectory(int(temp1), int(temp2))

	temperatureDirectoryText := strings.Join(temperatureDirectoryArray, "\n")

	errTemperatureDirectory := ioutil.WriteFile("temperature_directory.txt", []byte(temperatureDirectoryText), 0644)
	if errTemperatureDirectory != nil {
		log.Println(errTemperatureDirectory)
	}
}
