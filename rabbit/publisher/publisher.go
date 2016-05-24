package publisher

import (
	"fmt"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

// Publish is method for use in goroutines
func Publish(mainWg *sync.WaitGroup, in <-chan []byte, errs chan<- error, goroutines int, config Config) {
	defer mainWg.Done()

	done := make(chan bool, goroutines-1)
	stop := make(chan bool, goroutines-1)
	defer func() {
		close(done)
		close(stop)
	}()

	var conn connect
	err := conn.Init(config)
	if err != nil {
		panic(err)
	}
	defer conn.close()

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		c := conn.Copy()
		wg.Add(1)
		go func(wg *sync.WaitGroup, in <-chan []byte, errs chan<- error, conn connect) {
			defer func() {
				wg.Done()
				conn.close()
			}()

			for body := range in {
				err := conn.publishMessage(body)
				if err != nil {
					errs <- err
				}
			}

		}(&wg, in, errs, c)
	}
	wg.Wait()
}

// Config is type of config for connect
type Config struct {
	URL          string `yaml:"url"`
	QueueName    string `yaml:"queue_name"`
	ExchangeName string `yaml:"exchange_name"`
	ExchangeType string `yaml:"exchange_type"`
	RoutingKey   string `yaml:"routing_key"`
}

type connect struct {
	config       Config
	connection   *amqp.Connection
	channel      *amqp.Channel
	confirmation chan amqp.Confirmation
}

func (c *connect) dial() (err error) {
	c.connection, err = amqp.Dial(c.config.URL)
	return
}

func (c *connect) checkDefaultConfig() {
	if c.config.ExchangeType == `` {
		c.config.ExchangeType = `direct`
	}
}

// Init is method
func (c *connect) Init(Config Config) error {
	c.config = Config
	c.checkDefaultConfig()
	err := c.init()
	return err
}

// Copy is method
func (c *connect) Copy() (copy connect) {
	copy.config = c.config
	copy.init()
	return
}

func (c *connect) publishMessage(js []byte) error {
	var successSend bool
	for !successSend {

		if err := c.publishMessageInner(js); err != nil {
			return fmt.Errorf(`Непредвиденная ошибка publishMessageInner: %+v`, err)
		}

		confirmed := <-c.confirmation
		if confirmed.Ack == true {
			successSend = true
		} else {
			if err := c.reinit(); err != nil {
				return fmt.Errorf(`Непредвиденная ошибка reinit: %+v`, err)
			}
		}
	}
	return nil
}

func (c *connect) publishMessageInner(body []byte) (err error) {
	err = c.channel.Publish(
		c.config.ExchangeName, // publish to an exchange
		c.config.RoutingKey,   // routing to 0 or more queues
		false,                 // mandatory
		false,                 // immediate
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "text/plain",
			ContentEncoding: "",
			Body:            body,
			DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
			Priority:        0,              // 0-9
			// a bunch of application/implementation-specific fields
		},
	)
	return
}

func (c *connect) init() (err error) {
	err = c.dial()
	if err != nil {
		return fmt.Errorf("Amqp Dial: %s", err)
	}
	err = c.initChannel()
	if err != nil {
		return fmt.Errorf("Amqp Channel: %s", err)
	}
	return
}

func (c *connect) initChannel() (err error) {

	if c.channel, err = c.connection.Channel(); err != nil {
		return fmt.Errorf("Init Channel: %s", err)
	}

	c.confirmation = c.channel.NotifyPublish(make(chan amqp.Confirmation, 1))
	if err := c.channel.Confirm(false); err != nil {
		return fmt.Errorf("confirm.select destination: %s", err)
	}

	if err := c.channel.ExchangeDeclare(
		c.config.ExchangeName, // name
		c.config.ExchangeType, // type
		true,  // durable
		false, // auto-deleted
		false, // internal
		false, // noWait
		nil,   // arguments
	); err != nil {
		return fmt.Errorf("Exchange Declare: %s", err)
	}

	//TODO: bind Exchange and Queue

	if err := c.channel.Confirm(false); err != nil {
		return fmt.Errorf("Channel could not be put into confirm mode: %s", err)
	}

	return
}

func (c *connect) close() {
	c.channel.Close()
	c.connection.Close()
}

func (c *connect) reinit() (err error) {
	c.close()
	while := true
	for while {
		time.Sleep(time.Second * 5)
		if err := c.init(); err == nil {
			while = false
			err = nil
		}
	}
	return
}
