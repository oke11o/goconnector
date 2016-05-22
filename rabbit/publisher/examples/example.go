package main

import (
	"fmt"
	"io/ioutil"
	"strconv"

	"gopkg.in/yaml.v2"

	"github.com/oke11o/goconnector/rabbit/publisher"
)

func main() {
	var config publisher.Config
	readYaml(`config.yaml`, &config)

	in := make(chan []byte)
	done := make(chan bool)

	go publisher.Publish(in, done, 1, config)

	var body []byte
	for i := 0; i < 50000; i++ {
		fmt.Printf("\r%d", i)
		body = []byte(strconv.Itoa(i))
		in <- body
	}
	close(in)
	select {
	case <-done:
	}
	fmt.Println("Done script.")
}

func readYaml(filename string, obj interface{}) error {
	configBytes, err := ioutil.ReadFile(filename)
	if err != nil {
		e := fmt.Errorf("Ошибка в ReadFile, %v", err)
		return e
	}
	if err := yaml.Unmarshal(configBytes, obj); err != nil {
		e := fmt.Errorf("Ошибка в Unmarshal, %v", err)
		return e
	}
	return nil
}
