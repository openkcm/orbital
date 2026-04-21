package orbital_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/openkcm/orbital"
	"github.com/openkcm/orbital/respondertest"
)

func TestNewAsyncRunner(t *testing.T) {
	client := respondertest.NewResponder()

	tests := []struct {
		name   string
		opts   []orbital.AsyncOption
		expErr error
	}{
		{
			name:   "negative buffer size",
			opts:   []orbital.AsyncOption{orbital.WithBufferSize(-1)},
			expErr: orbital.ErrBufferSizeNegative,
		},
		{
			name:   "zero number of workers",
			opts:   []orbital.AsyncOption{orbital.WithNumberOfWorkers(0)},
			expErr: orbital.ErrNumberOfWorkersNotPositive,
		},
		{
			name: "without options",
			opts: []orbital.AsyncOption{},
		},
		{
			name: "with options",
			opts: []orbital.AsyncOption{
				orbital.WithBufferSize(0),
				orbital.WithNumberOfWorkers(1),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := orbital.NewAsyncRunner(client, tt.opts...)
			if tt.expErr != nil {
				assert.ErrorIs(t, err, tt.expErr)
				return
			}
			assert.NoError(t, err)
			assert.NotNil(t, r)
		})
	}
}

func TestNewAsyncRunner_NilResponder(t *testing.T) {
	r, err := orbital.NewAsyncRunner(nil)
	assert.Nil(t, r)
	assert.ErrorIs(t, err, orbital.ErrResponderNil)
}
