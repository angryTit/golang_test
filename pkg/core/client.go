package core

import (
	"context"
	"log"
	"time"
)

type ServiceClient interface {
	ProcessItems(ctx context.Context, items []Item) (int, error)
}

type serviceClient struct {
	service Service
	nextTry time.Duration
}

func NewServiceClient(service Service, nextTry time.Duration) ServiceClient {
	return &serviceClient{service: service, nextTry: nextTry}
}

// ProcessItems processes the given items using the Service client without exceeding the processing limits.
// It retrieves the processing limits from the Service client, creates batches of items, and processes them.
// If the processing limit is exceeded, it waits until the next batch can be processed before trying again.
//
// The function  returns an error if any error occurs during processing and the index of the last successfully processed item.
func (c *serviceClient) ProcessItems(ctx context.Context, items []Item) (int, error) {
	lastProcessedIndex := -1

	for len(items) > 0 {

		// Check if the context is cancelled
		select {
		case <-ctx.Done():
			return lastProcessedIndex, ctx.Err()
		default:
		}

		// Get the processing limits each time before processing next batch
		n, p := c.service.GetLimits()

		// Create a batch of objects
		batch := items[:n]

		// Process the batch
		err := c.service.Process(ctx, batch)

		// Calculate the time when the next batch can be processed
		nextBatchTime := time.Now().Add(p)

		if err == ErrBlocked {
			// If the limit is exceeded, wait until the next batch can be processed
			log.Printf("Processing blocked. Waiting until %s to next try", time.Now().Add(c.nextTry))
			time.Sleep(c.nextTry)
			continue
		} else if err != nil {
			return lastProcessedIndex, err
		}

		// Get the index of last processed item
		lastProcessedIndex = lastProcessedIndex + len(batch)
		// Remove processed items from the list
		items = items[n:]

		if len(items) == 0 {
			log.Printf("Successfully processed all %d items", lastProcessedIndex+1)
			return lastProcessedIndex, nil
		}

		// Wait until the next batch can be processed
		log.Printf("Processed %d items. Waiting until %s to process the next batch...", n, nextBatchTime)
		time.Sleep(nextBatchTime.Sub(time.Now()))
	}

	return lastProcessedIndex, nil
}
