package orbital

import (
	"encoding/json/v2"
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

// ToJSONString converts Labels to a JSON string for storage.
// Nil labels are encoded as JSON null, matching the format already
// persisted by encoding/json v1.
func (l Labels) ToJSONString() (string, error) {
	b, err := json.Marshal(l, json.FormatNilMapAsNull(true))
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// mergeLabels creates a new Labels map by merging multiple label maps.
// Later maps override earlier maps if keys conflict.
// No input maps are modified. Nil maps are skipped.
func mergeLabels(ls ...Labels) Labels {
	merged := make(Labels)
	for _, l := range ls {
		maps.Copy(merged, l)
	}
	return merged
}
