package mq

import "hash/fnv"

// PartitionFor maps a message key onto a partition in [0, n).
func PartitionFor(key string, n int) int {
	if n <= 0 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(n))
}
