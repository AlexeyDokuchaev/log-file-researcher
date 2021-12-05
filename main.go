package main

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/joho/godotenv"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"
)

var messagesChan chan []byte
var doneChan chan bool
var wg sync.WaitGroup
var cnt int
var errCnt int
var cfg Config

type Config struct {
	Root     string `envconfig:"PATH_ROOT" default:"/"`
	Url2Send string `envconfig:"URL_TO_SEND" default:"0.0.0.0:8000"`
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	cfg.Root = os.Getenv("PATH_ROOT")
	cfg.Url2Send = os.Getenv("URL_TO_SEND")

	cnt, errCnt = 0, 0

	doneChan = make(chan bool)
	messagesChan = make(chan []byte, 10000)

	root := cfg.Root

	wg.Add(1)
	go senderProcess()

	//for i := 1; i < 12; i++ {
	//	tmpCnt, tmpCntErr := cnt, errCnt
	//	viewFiles(root + strconv.Itoa(i))
	//	fmt.Println("app done", cnt-tmpCnt, errCnt-tmpCntErr)
	//}
	viewFiles(root + "second/SKU")

	for len(messagesChan) > 0 {
		<-time.After(time.Second)
	}

	close(doneChan)

	wg.Wait()

	fmt.Println("all done", cnt, errCnt)
}

func viewFiles(rootPath string) {
	var files []string
	err := filepath.Walk(rootPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			fmt.Println(err)
			return nil
		}

		if !info.IsDir() {
			files = append(files, path)
		}

		return nil
	})

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("count_files: ", len(files))
	for _, file := range files {
		fmt.Println(file)
		readFile(file)
		for len(messagesChan) > 0 {
			<-time.After(time.Millisecond * 300)
		}
	}
}

func readFile(filename string) {
	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}

	defer file.Close()

	re := regexp.MustCompile("_\\[(.*?)\\].")
	match := re.FindStringSubmatch(filename)
	date, _ := time.Parse("02.01.2006", match[1])
	dateByte := []byte("{\"date\":\"" + date.Format("2006-01-02") + "\",")

	reader := bufio.NewReader(file)
	var line []byte
	for {
		line, _, err = reader.ReadLine()
		if err == io.EOF {
			break
		}
		json := make([]byte, len(line)-28)

		copy(json, line[28:])
		json = append(dateByte, json...)

		if len(json) == 0 {
			fmt.Println("####len zero")
			continue
		}

		if string(json[len(json)-1:]) != "}" {
			fmt.Println("####unexpected end of line")
			continue
		}
		messagesChan <- json
		cnt++
		if cnt%100 == 0 {
			<-time.After(100 * time.Millisecond)
		}
		if cnt%10000 == 0 {
			fmt.Println("r-sts:", cnt)
			<-time.After(10 * time.Second)
		}
		if cnt%50000 == 0 {
			fmt.Println("r-sts:", cnt)
			<-time.After(3 * time.Minute)
		}
	}
}

func senderProcess() {
	defer wg.Done()

	for {
		select {
		case <-doneChan:
			return
		case msg := <-messagesChan:
			sendToServer(msg)
		}
	}
}

func sendToServer(msg []byte) {
	response, err := http.Post(cfg.Url2Send, "application/json", bytes.NewBuffer(msg))

	defer response.Body.Close()
	if err != nil {
		errCnt++
		fmt.Println("error: ", err.Error())
		if response != nil {
			fmt.Println(response.Status)
		}
		fmt.Println("error r-sts:", cnt)
		return
	}

	if response.StatusCode != http.StatusOK {
		errCnt++
	}

	return
}
