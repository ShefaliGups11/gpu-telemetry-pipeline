package mq

import "testing"

func TestAssignExclusive(t *testing.T) {
	as := Assign(8, []string{"b", "a"})
	if len(as["a"]) == 0 || len(as["b"]) == 0 {
		t.Fatalf("expected both members to get partitions: %+v", as)
	}
	seen := map[int]string{}
	for m, parts := range as {
		for _, p := range parts {
			if other, ok := seen[p]; ok {
				t.Fatalf("partition %d assigned to %s and %s", p, m, other)
			}
			seen[p] = m
		}
	}
	if len(seen) != 8 {
		t.Fatalf("expected 8 partitions assigned, got %d", len(seen))
	}
}

func TestAssignEmptyMembers(t *testing.T) {
	as := Assign(8, nil)
	if len(as) != 0 {
		t.Fatalf("expected empty map, got %+v", as)
	}
}

func TestAssignSortedRoundRobin(t *testing.T) {
	as := Assign(4, []string{"z", "a"})
	// sorted a,z so even partitions to a
	if got := as["a"]; len(got) != 2 || got[0] != 0 || got[1] != 2 {
		t.Fatalf("a partitions: %v", got)
	}
	if got := as["z"]; len(got) != 2 || got[0] != 1 || got[1] != 3 {
		t.Fatalf("z partitions: %v", got)
	}
}
