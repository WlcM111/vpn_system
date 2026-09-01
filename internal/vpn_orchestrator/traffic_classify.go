package vpn_orchestrator

import (
	kafkacontracts "vpn-platform/internal/contracts/kafka"
)

// ============================================================================
// Классификация отчёта о трафике.
//
// Вынесено из ApplyNodeTraffic в отдельную чистую функцию: разделение CDN и
// не-CDN — это то место, где ошибка стоит дороже всего (либо пользователь
// теряет оплаченный доступ, либо CDN становится безлимитным), а внутри
// транзакции оно было непроверяемо без поднятого Postgres.
//
// Функция не обращается ни к БД, ни к сети, ни к часам — результат полностью
// определяется входом.
// ============================================================================

// TrafficAggregate — результат разбора одного отчёта узла.
type TrafficAggregate struct {
	// TotalUplink/TotalDownlink — сумма по всем учёткам узла, для метрики.
	TotalUplink   int64
	TotalDownlink int64

	// PerUser — весь трафик пользователя на узле: [uplink, downlink].
	PerUser map[int64][2]int64

	// CDNPerUser — только CDN-часть: [uplink, downlink]. Пользователи без
	// CDN-трафика в карту не попадают.
	CDNPerUser map[int64][2]int64

	// Unclassified — сколько позиций пришло без inbound_tag. Это ожидаемо
	// ненулевая величина: основной и gRPC-профили делят один email, поэтому
	// агент осознанно не проставляет им тег. Алертить надо на резкий рост
	// относительно базовой линии, а не на любое ненулевое значение.
	Unclassified int
}

// classifyTrafficItems раскладывает позиции отчёта на общий и CDN-трафик.
//
// cdnInbounds — allowlist инбаундов, построенный из серверной конфигурации
// CDN-эндпоинтов. Пустой allowlist означает «классификация выключена»: тогда
// CDNPerUser остаётся пустым и в квоту не попадает ничего. Это сознательный
// fail-closed: лучше не начислить, чем начислить не то.
//
// Правила классификации, каждое из которых закрывает конкретный риск:
//
//	пустой inbound_tag — учётка принадлежит нескольким инбаундам, разделить
//	  трафик нечем; в CDN не идёт, считается в Unclassified;
//	тег вне allowlist — обычный VLESS, gRPC, Hysteria; в CDN не идёт;
//	тег в allowlist — CDN, uplink и downlink начисляются раздельно.
//
// Отображаемое имя, содержимое ссылки, позиция в списке и подстрока "cdn"
// в классификации не участвуют.
func classifyTrafficItems(
	items []kafkacontracts.VPNNodeTrafficItem,
	cdnInbounds map[string]struct{},
) TrafficAggregate {
	agg := TrafficAggregate{
		PerUser:    make(map[int64][2]int64, len(items)),
		CDNPerUser: make(map[int64][2]int64, len(items)),
	}

	for _, item := range items {
		if item.TelegramID == 0 {
			continue
		}

		agg.PerUser[item.TelegramID] = [2]int64{
			agg.PerUser[item.TelegramID][0] + item.Uplink,
			agg.PerUser[item.TelegramID][1] + item.Downlink,
		}
		agg.TotalUplink += item.Uplink
		agg.TotalDownlink += item.Downlink

		if len(cdnInbounds) == 0 {
			continue
		}
		if item.InboundTag == "" {
			agg.Unclassified++
			continue
		}
		if _, isCDN := cdnInbounds[item.InboundTag]; !isCDN {
			continue
		}
		agg.CDNPerUser[item.TelegramID] = [2]int64{
			agg.CDNPerUser[item.TelegramID][0] + item.Uplink,
			agg.CDNPerUser[item.TelegramID][1] + item.Downlink,
		}
	}

	return agg
}
