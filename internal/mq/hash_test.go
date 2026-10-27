package mq

import "testing"

func TestPartitionForStable(t *testing.T) {
	key := "GPU-5fd4f087-86f3-7a43-b711-4771313afc50"
	a := PartitionFor(key, 8)
	b := PartitionFor(key, 8)
	if a != b {
		t.Fatalf("expected stable partition, got %d vs %d", a, b)
	}
	if a < 0 || a >= 8 {
		t.Fatalf("partition out of range: %d", a)
	}
}

func TestPartitionForRange(t *testing.T) {
	seen := map[int]bool{}
	for i := 0; i < 200; i++ {
		p := PartitionFor(string(rune(i)), 8)
		if p < 0 || p >= 8 {
			t.Fatalf("out of range %d", p)
		}
		seen[p] = true
	}
	if len(seen) < 4 {
		t.Fatalf("hash not spreading: %v", seen)
	}
}
