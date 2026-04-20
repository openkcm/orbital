package orbital

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
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

// ToJSON converts Labels to JSON bytes for storage.
func (l Labels) ToJSON() ([]byte, error) {
	return json.Marshal(l)
}

// mergeLabels creates a new Labels map by merging multiple label maps.
// Later maps override earlier maps if keys conflict.
// No input maps are modified. Nil maps are skipped.
func mergeLabels(labelMaps ...Labels) Labels {
	merged := make(Labels)
	for _, labels := range labelMaps {
		maps.Copy(merged, labels)
	}
	return merged
}
