package middleware

import (
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func connect(settings ConnSettings) (*amqp.Connection, *amqp.Channel, error) {
	url := fmt.Sprintf(
		"amqp://guest:guest@%s:%d/",
		settings.Hostname,
		settings.Port,
	)

	var conn *amqp.Connection
	var err error

	for i := 0; i < 10; i++ {
		conn, err = amqp.Dial(url)
		if err == nil {
			break
		}
		time.Sleep(2 * time.Second)
	}

	if err != nil {
		return nil, nil, ErrMessageMiddlewareDisconnected
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, nil, ErrMessageMiddlewareDisconnected
	}

	err = ch.Qos(1, 0, false)
	if err != nil {
		return nil, nil, ErrMessageMiddlewareMessage
	}

	return conn, ch, nil
}
