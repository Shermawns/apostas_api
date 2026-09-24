package metrics

import (
	"fmt"
	"io"
	"sort"
	"sync"
)

var registry = struct {
	sync.Mutex
	values map[string]uint64
}{values: make(map[string]uint64)}

func Inc(name string) {
	registry.Lock()
	registry.values[name]++
	registry.Unlock()
}

func Add(name string, value uint64) {
	registry.Lock()
	registry.values[name] += value
	registry.Unlock()
}

func WritePrometheus(w io.Writer) error {
	registry.Lock()
	keys := make([]string, 0, len(registry.values))
	for key := range registry.values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if _, err := fmt.Fprintf(w, "%s %d\n", key, registry.values[key]); err != nil {
			registry.Unlock()
			return err
		}
	}
	registry.Unlock()
	return nil
}
