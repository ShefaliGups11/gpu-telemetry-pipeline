package mq

import "sort"

// Assign maps partition ids onto sorted member ids (round-robin).
// Each partition is owned by exactly one member when members is non-empty.
func Assign(npart int, members []string) map[string][]int {
	out := make(map[string][]int, len(members))
	if npart <= 0 || len(members) == 0 {
		for _, m := range members {
			out[m] = nil
		}
		return out
	}
	cp := append([]string(nil), members...)
	sort.Strings(cp)
	for _, m := range cp {
		out[m] = []int{}
	}
	for i := 0; i < npart; i++ {
		m := cp[i%len(cp)]
		out[m] = append(out[m], i)
	}
	return out
}
