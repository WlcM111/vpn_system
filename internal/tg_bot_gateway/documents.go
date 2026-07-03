package tg_bot_gateway

import (
	"context"
	"log"
	"sync"
)

// ============================================================================
// Хранилище ссылок на документы (Пользовательское соглашение, Публичная оферта,
// Политика конфиденциальности), опубликованные в Telegraph.
//
// При старте бота тексты из texts.go публикуются как Telegraph-страницы, а их
// URL кэшируются здесь. Меню «Описание услуг» отдаёт пользователю эти ссылки.
//
// Потокобезопасно: создаётся один раз при старте (запись), читается из обработчиков
// (чтение) — защищено sync.RWMutex.
// ============================================================================

// documentsStore хранит URL опубликованных документов.
type documentsStore struct {
	mu         sync.RWMutex
	ready      bool
	userURL    string // Пользовательское соглашение
	offerURL   string // Публичная оферта
	privacyURL string // Политика конфиденциальности
	refundURL  string // Политика возврата
}

// newDocumentsStore создаёт пустое хранилище.
func newDocumentsStore() *documentsStore {
	return &documentsStore{}
}

// publishAll публикует три документа в Telegraph и сохраняет их URL.
// Вызывается один раз при старте бота (в горутине, чтобы не блокировать запуск).
// Если публикация не удалась (нет сети и т.п.) — логируется, ready остаётся false,
// и меню документов будет сообщать о временной недоступности.
func (d *documentsStore) publishAll(ctx context.Context, tg *telegraphClient) {
	if err := tg.createAccount(ctx, "HouseVPN", "House VPN"); err != nil {
		log.Printf("[tg-bot] telegraph createAccount failed: %v", err)
		return
	}

	userURL, err := tg.createPage(ctx, "Пользовательское соглашение", "House VPN", userAgreementText())
	if err != nil {
		log.Printf("[tg-bot] telegraph publish user agreement failed: %v", err)
		return
	}

	offerURL, err := tg.createPage(ctx, "Публичная оферта", "House VPN", publicOfferText())
	if err != nil {
		log.Printf("[tg-bot] telegraph publish public offer failed: %v", err)
		return
	}

	privacyURL, err := tg.createPage(ctx, "Политика конфиденциальности", "House VPN", privacyPolicyText())
	if err != nil {
		log.Printf("[tg-bot] telegraph publish privacy policy failed: %v", err)
		return
	}

	refundURL, err := tg.createPage(ctx, "Политика возврата", "House VPN", refundPolicyText())
	if err != nil {
		log.Printf("[tg-bot] telegraph publish refund policy failed: %v", err)
		return
	}

	d.mu.Lock()
	d.userURL = userURL
	d.offerURL = offerURL
	d.privacyURL = privacyURL
	d.refundURL = refundURL
	d.ready = true
	d.mu.Unlock()

	log.Printf("[tg-bot] telegraph documents published: agreement=%s offer=%s privacy=%s refund=%s",
		userURL, offerURL, privacyURL, refundURL)
}

// links возвращает URL документов и флаг готовности.
func (d *documentsStore) links() (userURL, offerURL, privacyURL, refundURL string, ready bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.userURL, d.offerURL, d.privacyURL, d.refundURL, d.ready
}
