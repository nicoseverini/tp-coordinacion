package middleware

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

type queueMiddleware struct {
	conn      *amqp.Connection
	channel   *amqp.Channel
	queueName string

	deliveries <-chan amqp.Delivery
	done       chan struct{}
}

func (q *queueMiddleware) Send(msg Message) error {
	if q.channel == nil {
		return ErrMessageMiddlewareDisconnected
	}

	err := q.channel.Publish(
		"",
		q.queueName,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg.Body),
		},
	)
	if err != nil {
		return ErrMessageMiddlewareMessage
	}

	return nil
}

func (q *queueMiddleware) StartConsuming(
	callbackFunc func(msg Message, ack func(), nack func()),
) error {
	if q.channel == nil {
		return ErrMessageMiddlewareDisconnected
	}

	msgs, err := q.channel.Consume(
		q.queueName,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return ErrMessageMiddlewareDisconnected
	}

	q.deliveries = msgs
	q.done = make(chan struct{})
	return q.consumeLoop(callbackFunc)
}

func (q *queueMiddleware) StopConsuming() error {
	if q.done != nil {
		close(q.done)
		q.done = nil
	}
	return nil
}

func (q *queueMiddleware) Close() error {
	_ = q.StopConsuming()

	if q.channel != nil {
		err := q.channel.Close()
		if err != nil {
			return ErrMessageMiddlewareClose
		}
		q.channel = nil
	}

	if q.conn != nil {
		err := q.conn.Close()
		if err != nil {
			return ErrMessageMiddlewareClose
		}
		q.conn = nil
	}

	return nil
}

func NewQueueMiddleware(queueName string, settings ConnSettings) (Middleware, error) {
	conn, ch, err := connect(settings)
	if err != nil {
		return nil, err
	}

	_, err = ch.QueueDeclare(
		queueName,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, ErrMessageMiddlewareMessage
	}

	return &queueMiddleware{
		conn:      conn,
		channel:   ch,
		queueName: queueName,
	}, nil
}

func (q *queueMiddleware) consumeLoop(
	callbackFunc func(msg Message, ack func(), nack func()),
) error {

	for {
		select {

		case d, ok := <-q.deliveries:
			if !ok {
				return ErrMessageMiddlewareDisconnected
			}

			q.handleDelivery(d, callbackFunc)

		case <-q.done:
			return nil
		}
	}
}

func (q *queueMiddleware) handleDelivery(
	d amqp.Delivery,
	callbackFunc func(msg Message, ack func(), nack func()),
) {

	message := Message{
		Body: string(d.Body),
	}

	ack := func() {
		_ = d.Ack(false)
	}

	nack := func() {
		_ = d.Nack(false, true)
	}

	callbackFunc(message, ack, nack)
}
