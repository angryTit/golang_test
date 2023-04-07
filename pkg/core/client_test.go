package core

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestServiceClient_ProcessItems(t *testing.T) {
	t.Run("Successfully process all items", func(t *testing.T) {

		mockedService := &ServiceMock{
			GetLimitsFunc: func() (uint64, time.Duration) {
				return 3, 100 * time.Second
			},
			ProcessFunc: func(ctx context.Context, batch Batch) error {
				return nil
			},
		}

		client := NewServiceClient(mockedService, time.Second)

		items := []Item{
			{},
			{},
			{},
		}
		lastIndex, err := client.ProcessItems(context.Background(), items)
		require.NoError(t, err)
		require.EqualValues(t, 2, lastIndex)
	})

	t.Run("Successfully process all items in several calls", func(t *testing.T) {

		mockedService := &ServiceMock{
			GetLimitsFunc: func() (uint64, time.Duration) {
				return 1, 1 * time.Second
			},
			ProcessFunc: func(ctx context.Context, batch Batch) error {
				return nil
			},
		}

		client := NewServiceClient(mockedService, time.Second)

		items := []Item{
			{},
			{},
			{},
		}
		lastIndex, err := client.ProcessItems(context.Background(), items)
		require.NoError(t, err)
		require.EqualValues(t, 2, lastIndex)
	})

	t.Run("Items are not processed due to error", func(t *testing.T) {
		mockedService := &ServiceMock{
			GetLimitsFunc: func() (uint64, time.Duration) {
				return 1, 1 * time.Second
			},
			ProcessFunc: func(ctx context.Context, batch Batch) error {
				return errors.New("fail to process")
			},
		}

		client := NewServiceClient(mockedService, time.Second)

		items := []Item{
			{},
			{},
			{},
		}
		lastIndex, err := client.ProcessItems(context.Background(), items)
		require.Error(t, err)
		require.EqualValues(t, -1, lastIndex)
	})

	t.Run("Process blocked and exit by timeout", func(t *testing.T) {
		mockedService := &ServiceMock{
			GetLimitsFunc: func() (uint64, time.Duration) {
				return 1, 1 * time.Second
			},
			ProcessFunc: func(ctx context.Context, batch Batch) error {
				return ErrBlocked
			},
		}

		client := NewServiceClient(mockedService, time.Second)

		items := []Item{
			{},
			{},
			{},
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		lastIndex, err := client.ProcessItems(ctx, items)
		require.Error(t, err)
		require.EqualValues(t, -1, lastIndex)
	})

}
