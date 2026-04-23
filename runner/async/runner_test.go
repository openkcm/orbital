package async_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/openkcm/orbital"
	"github.com/openkcm/orbital/respondertest"
	"github.com/openkcm/orbital/runner/async"
)

var noopProcess orbital.ProcessFunc = func(_ context.Context, req orbital.TaskRequest) (orbital.TaskResponse, error) {
	return orbital.TaskResponse{TaskID: req.TaskID}, nil
}

func TestNew(t *testing.T) {
	client := respondertest.NewResponder()

	tests := []struct {
		name   string
		opts   []async.Option
		expErr error
	}{
		{
			name:   "negative buffer size",
			opts:   []async.Option{async.WithBufferSize(-1)},
			expErr: async.ErrBufferSizeNegative,
		},
		{
			name:   "zero number of workers",
			opts:   []async.Option{async.WithNumberOfWorkers(0)},
			expErr: async.ErrNumberOfWorkersNotPositive,
		},
		{
			name: "without options",
			opts: []async.Option{},
		},
		{
			name: "with options",
			opts: []async.Option{
				async.WithBufferSize(0),
				async.WithNumberOfWorkers(1),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := async.New(client, noopProcess, tt.opts...)
			if tt.expErr != nil {
				assert.ErrorIs(t, err, tt.expErr)
				return
			}
			assert.NoError(t, err)
			assert.NotNil(t, r)
		})
	}
}

func TestNew_NilResponder(t *testing.T) {
	r, err := async.New(nil, noopProcess)
	assert.Nil(t, r)
	assert.ErrorIs(t, err, async.ErrResponderNil)
}

func TestNew_NilProcessFunc(t *testing.T) {
	client := respondertest.NewResponder()
	r, err := async.New(client, nil)
	assert.Nil(t, r)
	assert.ErrorIs(t, err, async.ErrProcessFuncNil)
}
