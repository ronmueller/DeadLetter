package main

import (
	"log"
	"math/rand"
	"strconv"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	// 10 Messages with random Priorities between 0 and 9
	header := make(amqp.Table)
	header["x-delay"] = "5000"
	for msgs := 0; msgs < 10; msgs++ {
		prio := rand.Intn(31)
		err = ch.Publish(
			"msgs", // exchange
			"msg",  // routing key
			false,  // mandatory
			false,  // immediate
			amqp.Publishing{
				Headers:         amqp.Table{},
				ContentType:     "text/plain",
				ContentEncoding: "",
				Body:            []byte("test" + strconv.Itoa(msgs)),
				DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
			})
		log.Printf(" [x] Sent %d", prio)
		failOnError(err, "Failed to publish a message")
	}
}
