package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/ajtfj/graph"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	GRAPH_FILE = "graph.txt"
)

var (
	g *graph.Graph
)

func parceGraphInputLine(inputLine string) (graph.Node, graph.Node, int, error) {
	matches := strings.Split(inputLine, " ")
	if len(matches) < 3 {
		return graph.Node(""), graph.Node(""), 0, fmt.Errorf("invalid input")
	}

	weight, err := strconv.ParseInt(matches[2], 10, 0)
	if err != nil {
		return graph.Node(""), graph.Node(""), 0, err
	}

	return graph.Node(matches[0]), graph.Node(matches[1]), int(weight), nil
}

func setupGraph() error {
	g = graph.NewGraph()

	file, err := os.Open(GRAPH_FILE)
	if err != nil {
		return err
	}

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)

	for scanner.Scan() {
		inputLine := scanner.Text()
		u, v, weight, err := parceGraphInputLine(inputLine)
		if err != nil {
			return err
		}
		g.AddEdge(u, v, weight)
	}

	return nil
}

func init() {
	err := setupGraph()
	if err != nil {
		log.Fatal(err)
	}
}

func parseError(err error) []byte {
	errorPayload := ResponsePayload{
		Error: err,
	}
	errorData, err := json.Marshal(errorPayload)
	if err != nil {
		log.Fatal(err)
	}
	return errorData
}

func handleRequest(requestData []byte) ([]byte, *uuid.UUID) {
	requestPayload := RequestPayload{}
	if err := json.Unmarshal(requestData, &requestPayload); err != nil {
		return parseError(err), nil
	}

	path, err := g.ShortestPath(requestPayload.Ori, requestPayload.Dest)
	if err != nil {
		return parseError(err), nil
	}

	responsePayload := ResponsePayload{
		Path: path,
	}
	responseData, err := json.Marshal(responsePayload)
	if err != nil {
		return parseError(err), nil
	}

	return responseData, &requestPayload.ClientUUID
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Panic(err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Panic(err)
	}
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"responses", // name
		"direct",    // type
		true,        // durable
		false,       // auto-deleted
		false,       // internal
		false,       // no-wait
		nil,         // arguments
	)
	if err != nil {
		log.Panic(err)
	}

	requestsQueue, err := ch.QueueDeclare(
		"requests", // name
		false,      // durable
		false,      // delete when unused
		false,      // exclusive
		false,      // no-wait
		nil,        // arguments
	)
	if err != nil {
		log.Panic(err)
	}

	requests, err := ch.Consume(
		requestsQueue.Name, // queue
		"",                 // consumer
		true,               // auto-ack
		false,              // exclusive
		false,              // no-local
		false,              // no-wait
		nil,                // args
	)
	if err != nil {
		log.Panic(err)
	}

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")

	for d := range requests {
		log.Printf("Received a message: %s", d.Body)

		response, clientUUID := handleRequest(d.Body)
		if err := ch.PublishWithContext(context.Background(),
			"responses",         // exchange
			clientUUID.String(), // routing key
			false,               // mandatory
			false,               // immediate
			amqp.Publishing{
				ContentType: "application/json",
				Body:        response,
			},
		); err != nil {
			log.Fatal(err)
		}
	}
}

type RequestPayload struct {
	Ori        graph.Node `json:"ori"`
	Dest       graph.Node `json:"dest"`
	ClientUUID uuid.UUID  `json:"client_uuid"`
}

type ResponsePayload struct {
	Path  []graph.Node `json:"path"`
	Error error        `json:"error"`
}
