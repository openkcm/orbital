package orbital

import (
	"errors"
	"fmt"
	"strings"
)

// LabelPrefixReserved is the prefix reserved for internal orbital labels.
const LabelPrefixReserved = "orbital/"

// ErrReservedLabelPrefix is returned when a label key uses the reserved prefix.
var ErrReservedLabelPrefix = errors.New("label key with 'orbital/' prefix is reserved for internal use")

// Labels represents a map of key-value pairs for metadata.
type Labels map[string]string

// Validate checks that no labels use the reserved prefix.
// Returns nil if labels is nil or empty.
func (l Labels) Validate() error {
	for key := range l {
		if strings.HasPrefix(key, LabelPrefixReserved) {
			return fmt.Errorf("%w: %s", ErrReservedLabelPrefix, key)
		}
	}
	return nil
}
