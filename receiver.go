package main

import (
	"fmt"
	"log"

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

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")

	// -----------------------------------------------------------
	// Normal Message Exchange and Queue
	err = ch.ExchangeDeclare(
		"standard.exchange", // name
		"topic",             // type
		false,               // durable
		false,               // auto-deleted
		false,               // internal
		false,               // no-wait
		nil,                 // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	// Dead Letter Exchange
	args := make(amqp.Table)
	args["x-delayed-type"] = "topic"
	err = ch.ExchangeDeclare(
		"dlx.exchange",      // name
		"x-delayed-message", // type
		false,               // durable
		false,               // auto-deleted
		false,               // internal
		false,               // no-wait
		args,                // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	// -----------------------------------------------------------
	args = make(amqp.Table)
	args["x-dead-letter-exchange"] = "dlx.exchange"
	_, err = ch.QueueDeclare(
		"standard.queue", // name
		false,            // durable
		false,            // delete when unused
		false,            // exclusive
		false,            // no-wait
		args,             // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.QueueBind(
		"standard.queue",    // queue name
		"#",                 // routing key
		"standard.exchange", // exchange
		false,
		nil)
	failOnError(err, "Failed to bind a queue")
	err = ch.QueueBind(
		"standard.queue", // queue name
		"#",              // routing key
		"dlx.exchange",   // exchange
		false,
		nil)
	failOnError(err, "Failed to bind a queue")

	// args = make(amqp.Table)
	// // args["x-message-ttl"] = int32(5000)
	// args["x-dead-letter-exchange"] = "msgs"
	// _, err = ch.QueueDeclare(
	// 	"msgs.retry", // name
	// 	false,        // durable
	// 	false,        // delete when unused
	// 	false,        // exclusive
	// 	false,        // no-wait
	// 	args,         // arguments
	// )
	// failOnError(err, "Failed to declare a queue")

	// err = ch.QueueBind(
	// 	"msgs.retry",       // queue name
	// 	"",                 // routing key
	// 	"msgs.dead.letter", // exchange
	// 	false,
	// 	nil)
	// failOnError(err, "Failed to bind a queue")

	// -------------------------
	// Get Messages from normal Queue
	msgs, err := ch.Consume(
		"standard.queue", // queue
		"",               // consumer
		false,            // auto-ack
		false,            // exclusive
		false,            // no-local
		false,            // no-wait
		nil,              // args
	)
	failOnError(err, "Failed to register a consumer")

	defer ch.Close()
	defer conn.Close()

	forever := make(chan bool)

	// read message from queue
	go func() {
		for d := range msgs {
			xdeath := d.Headers["x-death"]
			log.Printf("Received a message: %s\n", d.Body)
			log.Printf("Headerinfo: %+v\n", xdeath)

			// check if death headers exist
			if v, ok := d.Headers["x-death"]; ok {
				// check if headerdata exist
				if _, ok := v.([]interface{}); ok {

					// read retry count from header
					switch retry := xdeath.([]interface{})[0].(amqp.Table)["count"].(type) {
					case int64:

						// acknowledge only after 3 retries
						if retry >= int64(3) {
							d.Ack(false)
						} else {

							// not 5 retries reached
							// send message back to the dead letter exchange
							fmt.Printf("retry: %d\n\n", retry)
							// d.ttl = int32(10000)
							d.Nack(false, false)
						}
					default:
						// if something went wrong in the header
						// delete message
						d.Ack(false)
					}
				}
			} else {
				// we have no header information on the first
				// message read atempt
				// send message to the dead letter exchange
				d.Nack(false, false)
			}

		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
