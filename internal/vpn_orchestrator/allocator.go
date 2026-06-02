package vpn_orchestrator

import (
	"log/slog"
	"sort"
	"time"
)

// Allocator выбирает, какие пул-айтемы и на каких нодах выдать пользователю.
// Модель «пул серверов одной страны»: пользователь видит ОДИН профиль на страну
// (меню локаций), а внутри страны нода выбирается балансировщиком
// (weighted least-load + health + sticky + soft overflow).
type Allocator struct {
	heartbeatTTL time.Duration
	softOverflow bool
}

// NewAllocator создаёт балансировщик.
//
//	heartbeatTTL — макс. возраст heartbeat, при котором нода считается живой.
//	softOverflow — если true и все ноды страны переполнены, всё равно выдать
//	               наименее загруженную (деградация лучше отказа). Если false —
//	               переполненная страна исключается из фида.
func NewAllocator(heartbeatTTL time.Duration, softOverflow bool) *Allocator {
	if heartbeatTTL <= 0 {
		heartbeatTTL = 90 * time.Second
	}
	return &Allocator{heartbeatTTL: heartbeatTTL, softOverflow: softOverflow}
}

// SortPoolItems — стабильная сортировка по sort_order (для предсказуемого порядка в фиде).
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

// stickyResolver сообщает, к какой ноде пользователь уже привязан для item_key.
type stickyResolver func(itemKey string) string

// Allocate выбирает финальный список пул-айтемов (по одному на страну).
//
//  1. Группируем включённые пул-айтемы по стране.
//  2. В каждой стране делим ноды на живые-со-ёмкостью и живые-переполненные (overflow).
//  3. Sticky: если пользователь уже на живой ноде этой страны со ёмкостью — оставляем её.
//  4. Иначе берём ноду с наименьшим LoadScore среди со-ёмкостью.
//  5. Если со-ёмкостью нод нет:
//     - softOverflow=true  → берём наименее загруженную из переполненных (+ warn в лог);
//     - softOverflow=false → страну в фид не добавляем.
//  6. Итог сортируем по sort_order.
func (a *Allocator) Allocate(items []PoolItem, servers map[string][]ServerLoad, sticky stickyResolver, now time.Time) []PoolItem {
	byKey := make(map[string]ServerLoad)
	for _, list := range servers {
		for _, s := range list {
			byKey[s.ServerKey] = s
		}
	}

	byCountry := make(map[string][]PoolItem)
	countryOrder := make([]string, 0)
	seen := make(map[string]bool)
	for _, it := range items {
		if !it.Enabled {
			continue
		}
		if !seen[it.CountryCode] {
			seen[it.CountryCode] = true
			countryOrder = append(countryOrder, it.CountryCode)
		}
		byCountry[it.CountryCode] = append(byCountry[it.CountryCode], it)
	}

	type scored struct {
		item  PoolItem
		score float64
	}

	chosen := make([]PoolItem, 0, len(countryOrder))

	for _, country := range countryOrder {
		candidates := byCountry[country]
		if len(candidates) == 0 {
			continue
		}

		withCapacity := make([]scored, 0, len(candidates))
		overflow := make([]scored, 0, len(candidates))
		for _, it := range candidates {
			s, ok := byKey[it.ServerKey]
			if !ok {
				continue // нет метрик ноды — пропускаем
			}
			if !s.AliveByHeartbeat(now, a.heartbeatTTL) {
				continue // мёртвая нода — исключаем всегда
			}
			if s.HasCapacity() {
				withCapacity = append(withCapacity, scored{item: it, score: s.LoadScore()})
			} else {
				overflow = append(overflow, scored{item: it, score: s.LoadScore()})
			}
		}

		pool := withCapacity
		if len(pool) == 0 {
			// Все живые ноды страны переполнены.
			if a.softOverflow && len(overflow) > 0 {
				slog.Warn("vpn-orchestrator all nodes over capacity in country, using overflow (consider adding a node)",
					"country", country, "overflow_nodes", len(overflow))
				pool = overflow
			} else {
				// softOverflow выключен или живых нод нет вообще — страну пропускаем.
				continue
			}
		}

		// Sticky: пользователь уже на одной из нод этого пула?
		var pick *PoolItem
		for i := range pool {
			bound := sticky(pool[i].item.ItemKey)
			if bound != "" && bound == pool[i].item.ServerKey {
				pick = &pool[i].item
				break
			}
		}

		// Иначе — наименее загруженная (least-load), при равенстве стабильно по sort_order/id.
		if pick == nil {
			sort.SliceStable(pool, func(i, j int) bool {
				if pool[i].score == pool[j].score {
					if pool[i].item.SortOrder == pool[j].item.SortOrder {
						return pool[i].item.ID < pool[j].item.ID
					}
					return pool[i].item.SortOrder < pool[j].item.SortOrder
				}
				return pool[i].score < pool[j].score
			})
			pick = &pool[0].item
		}

		chosen = append(chosen, *pick)
	}

	return a.SortPoolItems(chosen)
}
