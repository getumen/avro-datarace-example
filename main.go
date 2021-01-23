package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"

	"github.com/hamba/avro/ocf"
	"github.com/linkedin/goavro"
)

const userNum = 10000
const threadNum = 10

func main() {

	ctx := context.Background()

	wg := sync.WaitGroup{}

	out := make(chan Record)

	for i := 0; i < threadNum; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reader := generateOCF(ctx)

			dec, err := ocf.NewDecoder(reader)
			if err != nil {
				log.Fatalf("fail to create new decoder: %+v", err)
			}

			for dec.HasNext() {
				var record Record

				err = dec.Decode(&record)
				if err != nil {
					log.Fatalf("GenerateRecord: %+v", err)
				}

				out <- record
			}
		}()
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	counter := 0
	for range out {
		counter++
	}
	if counter != userNum*threadNum {
		log.Fatalf("expected %d, but got %d", userNum*threadNum, counter)
	}
}

// Record is user ID record
type Record struct {
	userID *string `avro:"user_id"`
}

func generateOCF(ctx context.Context) io.ReadCloser {
	const schema = `{
		"namespace": "com.example",
		"name": "datarace",
		"type": "record",
		"fields": [
			{"name": "user_id", "type": ["null", "string"]}
		]
	}`

	r, w := io.Pipe()

	go func() {
		defer w.Close()

		codec, err := goavro.NewCodec(schema)
		if err != nil {
			log.Fatalf("fail to create codec: %+v", err)
		}

		config := goavro.OCFConfig{
			W:               w,
			Codec:           codec,
			CompressionName: goavro.CompressionSnappyLabel,
		}
		writer, err := goavro.NewOCFWriter(config)
		if err != nil {
			log.Fatalf("fail to create writer: %+v", err)
		}

		block := make([]interface{}, 0)

		for j := 0; j < userNum; j++ {

			const featureNum = 100

			block = append(block, map[string]interface{}{
				"user_id": goavro.Union("string", fmt.Sprintf("userID-%d", j)),
			})
		}

		if err = writer.Append(block); err != nil {
			log.Fatalf("fail to append avro file: %+v", err)
		}
	}()

	return r
}
