package orbital_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/openkcm/orbital"
)

func TestHandlerResponse_WorkingState(t *testing.T) {
	// given
	tests := []struct {
		name            string
		pair            map[string]string
		rawWorkingState func(pair map[string]string) []byte
		expWorkingState func(pair map[string]string) *orbital.WorkingState
		expErr          error
	}{
		{
			name: "nil raw working state",
			rawWorkingState: func(_ map[string]string) []byte {
				return nil
			},
			expWorkingState: func(_ map[string]string) *orbital.WorkingState {
				return &orbital.WorkingState{}
			},
		},
		{
			name: "empty raw working state",
			rawWorkingState: func(_ map[string]string) []byte {
				return []byte("{}")
			},
			expWorkingState: func(_ map[string]string) *orbital.WorkingState {
				return &orbital.WorkingState{}
			},
		},
		{
			name: "invalid raw working state",
			rawWorkingState: func(_ map[string]string) []byte {
				return []byte("{invalid json}")
			},
			expWorkingState: func(_ map[string]string) *orbital.WorkingState {
				return nil
			},
			expErr: orbital.ErrWorkingStateInvalid,
		},
		{
			name: "valid raw working state",
			pair: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
			rawWorkingState: func(pair map[string]string) []byte {
				var sb strings.Builder
				for k, v := range pair {
					if sb.Len() > 0 {
						sb.WriteString(",")
					}
					sb.WriteString(`"` + k + `":"` + v + `"`)
				}
				return []byte(`{` + sb.String() + `}`)
			},
			expWorkingState: func(pair map[string]string) *orbital.WorkingState {
				ws := &orbital.WorkingState{}
				for k, v := range pair {
					ws.Set(k, v)
				}
				return ws
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := orbital.HandlerResponse{}
			resp.UseRawWorkingState(tt.rawWorkingState(tt.pair))

			// when
			ws, err := resp.WorkingState()

			// then
			if tt.expErr != nil {
				assert.ErrorIs(t, err, tt.expErr)
				assert.Nil(t, ws)
				return
			}
			assert.NoError(t, err)
			assert.NotNil(t, ws)

			expWorkingState := tt.expWorkingState(tt.pair)
			for k := range tt.pair {
				expVal, ok := expWorkingState.Value(k)
				assert.True(t, ok)
				actVal, ok := ws.Value(k)
				assert.True(t, ok)
				assert.Equal(t, expVal, actVal)
			}

			// when called again, should return the same working state
			ws2, err := resp.WorkingState()

			// then
			assert.NoError(t, err)
			assert.Equal(t, ws, ws2)
		})
	}
}
