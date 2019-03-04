package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/streadway/amqp"
)

// Queues struct which contains an array of queues
type Queues struct {
	Queues []Queue `json:"queues"`
}

// Queue struct which contains a name a type and a list of Queues
type Queue struct {
	RabbitMQConnectionString string `json:"RabbitMQConnectionString"`
	RabbitMQRequestQueueName string `json:"RabbitMQRequestQueueName"`
	RabbitMQExchangeName     string `json:"RabbitMQExchangeName"`
	RabbitMQDataQueueName    string `json:"RabbitMQDataQueueName"`
}

func ProcessQueue(req1 string) string {
	return fmt.Sprintf("%s -processed", req1)
}

//declaring queues globally
var queues Queues

func LoadConfigurationFile(filename string) string {
	// Open our jsonFile
	jsonFile, err := os.Open("configuration.json")
	// if we os.Open returns an error then handle it
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println("Successfully Opened configuration.json")
	// defer the closing of our jsonFile so that we can parse it later on
	defer jsonFile.Close()

	// read our opened xmlFile as a byte array.

	// we unmarshal our byteArray which contains our
	// jsonFile's content into 'users' which we defined above
	byteValue, _ := ioutil.ReadAll(jsonFile)
	json.Unmarshal(byteValue, &queues)
	return filename
}
func main() {
	LoadConfigurationFile("configuration.json")
	conn, err := amqp.Dial(queues.Queues[0].RabbitMQConnectionString)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		queues.Queues[0].RabbitMQRequestQueueName, //name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set QoS")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body) //any kind of further processing code
			b := d.Body
			s := string(b)
			fmt.Println(s)
			output := ProcessQueue(s)
			gpsCollectorProcessing(output)
			log.Printf("Done")
			d.Ack(true)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
