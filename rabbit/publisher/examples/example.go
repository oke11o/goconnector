package main

import (
	"fmt"
	"io/ioutil"
	"strconv"
	"sync"

	"gopkg.in/yaml.v2"

	"github.com/oke11o/goconnector/rabbit/publisher"
)

func main() {
	var config publisher.Config
	readYaml(`config.yaml`, &config)

	in := make(chan []byte, 10000)
	errs := make(chan error)

	var wg sync.WaitGroup

	wg.Add(1)
	go publisher.Publish(&wg, in, errs, 1, config)
	go func(errs <-chan error) {
		for e := range errs {
			fmt.Println(e)
		}
	}(errs)

	var body []byte
	for i := 0; i < 50000; i++ {
		fmt.Printf("\r%d", i)
		body = []byte(strconv.Itoa(i))
		in <- body
	}
	close(in)
	fmt.Println("close.")
	wg.Wait()
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
