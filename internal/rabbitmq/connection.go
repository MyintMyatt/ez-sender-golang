package rabbitmq

import (
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	// Exchanges
	MainExchange = "ez-sender.main.exchange"
	DlxExchange  = "ez-sender.dlx.exchange"

	// Security Queues
	SecurityEmailQueue = "q.security.email"
	SecuritySmsQueue   = "q.security.sms"
	SecurityFCMQueue   = "q.security.fcm"

	// Advertising Queues ( e.g welcome email, marketing email, etc)
	AdvertisingEmailQueue = "q.advertising.email"
	AdvertisingSmsQueue   = "q.advertising.sms"
	AdvertisingFCMQueue   = "q.advertising.fcm"

	// User Queues
	UserEmailQueue = "q.user.email"
	UserSmsQueue   = "q.user.sms"
	UserFCMQueue   = "q.user.fcm"

	// Retry Queues
	RetryEmailQueue = "q.retry.email"
	RetrySmsQueue   = "q.retry.sms"
	RetryFCMQueue   = "q.retry.fcm"

	// Dead Letter Queue
	DlqQueue      = "q.dlx.queue"
	DlxRoutingKey = "ez-sender.dlx.routing.key"
)

type RabbitMQClient struct {
	connection *amqp.Connection
	channel    *amqp.Channel
	url        string
}

func NewRabbitMQClient(url string) (*RabbitMQClient, error) {
	client := &RabbitMQClient{url: url}
	notifyClose, err := client.connect()
	if err != nil {
		return nil, err
	}

	if err := client.setUpTopology(); err != nil {
		log.Printf("Topology setup failed: %v", err)
		return nil, err
	}

	go client.handleReconnect(notifyClose)
	return client, nil
}

func (c *RabbitMQClient) connect() (chan *amqp.Error, error) {
	var err error
	for i := 0; i < 5; i++ {
		c.connection, err = amqp.Dial(c.url)
		if err == nil {
			break
		}
		time.Sleep(time.Duration(i+1) * time.Second)
		log.Printf("Failed to connect to RabbitMQ on attempt %d: %v", i+1, err,)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ after 5 attempts: %w", err)
	}

	c.channel, err = c.connection.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open a channel: %w", err)
	}

	notifyClose := make(chan *amqp.Error, 1)
	c.channel.NotifyClose(notifyClose)
	return notifyClose, nil
}

func (c *RabbitMQClient) handleReconnect(notifyClose chan *amqp.Error) {
	for err := range notifyClose {
		if err == nil {
			return
		}
		log.Printf("Rabbitmq connection lost: %v", err)

		for {
			time.Sleep(3 * time.Second)

			newNotifyClose, err := c.connect()
			if err != nil {
				log.Printf("Failed to reconnect to RabbitMQ: %v", err)
				continue
			}

			if err := c.setUpTopology(); err != nil {
				log.Printf("Topology setup failed: %v", err)
				c.channel.Close()
				c.connection.Close()
				continue
			}

			log.Println("RabbitMQ reconnected successfully")

			notifyClose = newNotifyClose
			break
		}
	}
}

func (c *RabbitMQClient) setUpTopology() error {

	if err := c.channel.ExchangeDeclare(MainExchange, "topic", true, false, false, false, nil); err != nil {
		return err
	}

	if err := c.channel.ExchangeDeclare(DlxExchange, "topic", true, false, false, false, nil); err != nil {
		return err
	}

	if _, err := c.channel.QueueDeclare(DlqQueue, true, false, false, false, nil); err != nil {
		return err
	}

	if err := c.channel.QueueBind(DlqQueue, DlxRoutingKey, DlxExchange, false, nil); err != nil {
		return err
	}

	queueArgs := amqp.Table{
		"x-max-priority" : 5,
		
		"x-dead-letter-exchange":    DlxExchange,
		"x-dead-letter-routing-key": DlxRoutingKey,
	}

	// Security Queues
	if _, err := c.channel.QueueDeclare(SecurityEmailQueue, true, false, false, false, queueArgs); err != nil {
		return err
	}

	if _, err := c.channel.QueueDeclare(SecuritySmsQueue, true, false, false, false, queueArgs); err != nil {
		return err
	}

	if _, err := c.channel.QueueDeclare(SecurityFCMQueue, true, false, false, false, queueArgs); err != nil {
		return err
	}

	// Security Queue Bindings
	if err := c.channel.QueueBind(SecurityEmailQueue, "security.*.email", MainExchange, false, nil); err != nil {
		return err
	}

	if err := c.channel.QueueBind(SecuritySmsQueue, "security.*.sms", MainExchange, false, nil); err != nil {
		return err
	}

	if err := c.channel.QueueBind(SecurityFCMQueue, "security.*.fcm", MainExchange, false, nil); err != nil {
		return err
	}

	// Advertising Queues
	if _, err := c.channel.QueueDeclare(AdvertisingEmailQueue, true, false, false, false, queueArgs); err != nil {
		return err
	}
	if _, err := c.channel.QueueDeclare(AdvertisingSmsQueue, true, false, false, false, queueArgs); err != nil {
		return err
	}
	if _, err := c.channel.QueueDeclare(AdvertisingFCMQueue, true, false, false, false, queueArgs); err != nil {
		return err
	}

	// Advertising Queue Bindings
	if err := c.channel.QueueBind(AdvertisingEmailQueue, "advertising.*.email", MainExchange, false, nil); err != nil {
		return err
	}

	if err := c.channel.QueueBind(AdvertisingSmsQueue, "advertising.*.sms", MainExchange, false, nil); err != nil {
		return err
	}

	if err := c.channel.QueueBind(AdvertisingFCMQueue, "advertising.*.fcm", MainExchange, false, nil); err != nil {
		return err
	}

	// User Queues
	if _, err := c.channel.QueueDeclare(UserEmailQueue, true, false, false, false, queueArgs); err != nil {
		return err
	}
	if _, err := c.channel.QueueDeclare(UserSmsQueue, true, false, false, false, queueArgs); err != nil {
		return err
	}
	if _, err := c.channel.QueueDeclare(UserFCMQueue, true, false, false, false, queueArgs); err != nil {
		return err
	}

	// User Queue Bindings
	if err := c.channel.QueueBind(UserEmailQueue, "user.*.email", MainExchange, false, nil); err != nil {
		return err
	}
	if err := c.channel.QueueBind(UserSmsQueue, "user.*.sms", MainExchange, false, nil); err != nil {
		return err
	}
	if err := c.channel.QueueBind(UserFCMQueue, "user.*.fcm", MainExchange, false, nil); err != nil {
		return err
	}


	// Retry Queues & Bindings
	retryArgs := amqp.Table {
		"x-message-ttl" : int32(30_000),
		"x-dead-letter-exchange": MainExchange,
	}

	if _, err := c.channel.QueueDeclare(RetryEmailQueue, true, false, false, false, retryArgs); err != nil {
		return err
	}
	if _, err := c.channel.QueueDeclare(RetrySmsQueue, true, false, false, false, retryArgs); err != nil {
		return err
	}
	if _, err := c.channel.QueueDeclare(RetryFCMQueue, true, false, false, false, retryArgs); err != nil {
		return err
	}

	if err := c.channel.QueueBind(RetryEmailQueue, "retry.*.email", MainExchange, false, nil); err != nil {
		return err
	}
	if err := c.channel.QueueBind(RetrySmsQueue, "retry.*.sms", MainExchange, false, nil); err != nil {
		return err
	}
	if err := c.channel.QueueBind(RetryFCMQueue, "retry.*.fcm", MainExchange, false, nil); err != nil {
		return err
	}

	return nil
}

func (c *RabbitMQClient) Close() error {
	if c.channel != nil {
		if err := c.channel.Close(); err != nil {
			return fmt.Errorf("failed to close channel: %w", err)
		}
	}

	if c.connection != nil {
		if err := c.connection.Close(); err != nil {
			return fmt.Errorf("failed to close connection: %w", err)
		}
	}
	return nil
}
