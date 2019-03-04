package main

import (
	"log"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
func gpsCollectorProcessing(output string) {
	conn, err := amqp.Dial(queues.Queues[0].RabbitMQConnectionString)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		queues.Queues[0].RabbitMQDataQueueName, // name
		true,                                   // durable
		false,                                  // delete when unused
		false,                                  // exclusive
		false,                                  // no-wait
		nil,                                    // arguments
	)
	failOnError(err, "Failed to declare a queue")
	err = ch.ExchangeDeclare(
		queues.Queues[0].RabbitMQExchangeName, // name
		"direct",                              // type
		true,                                  // durable
		false,                                 // auto-deleted
		false,                                 // internal
		false,                                 // no-wait
		nil,                                   // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	err = ch.Publish(
		queues.Queues[0].RabbitMQExchangeName, // exchange
		q.Name,                                // routing key
		false,                                 // mandatory
		false,                                 // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         []byte(output),
		})

	failOnError(err, "Failed to publish a message")
	log.Printf(" [x] Sent %s", output)
}
