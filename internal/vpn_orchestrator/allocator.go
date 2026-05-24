package vpn_orchestrator

import "sort"

type Allocator struct{}

func NewAllocator() *Allocator {
	return &Allocator{}
}

func (a *Allocator) SortPoolItems(items []PoolItem) []PoolItem {
	out := make([]PoolItem, len(items))
	copy(out, items)
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].SortOrder == out[j].SortOrder {
			return out[i].ID < out[j].ID
		}
		return out[i].SortOrder < out[j].SortOrder
	})
	return out
}
