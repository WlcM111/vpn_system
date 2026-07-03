package tg_bot_gateway

import (
	"context"
	"log"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

// ============================================================================
// Обработчики раздела «Описание услуг» и документов (Telegraph-ссылки).
// ============================================================================

// handleServicesInfo открывает подменю «Описание услуг» с тремя документами.
func (a *App) handleServicesInfo(ctx context.Context, chatID int64, state *ChatState) {
	state.Step = StepServicesMenu
	_ = a.stateStore.Set(ctx, chatID, state)
	a.sendText(chatID, servicesDescriptionText(), servicesMenuKeyboard())
}

// handleServicesMenuChoice обрабатывает выбор в подменю «Описание услуг».
func (a *App) handleServicesMenuChoice(ctx context.Context, chatID int64, state *ChatState, text string) {
	switch text {
	case btnDocUser:
		a.sendDocumentLink(chatID, "Пользовательское соглашение", docKindUser)
	case btnDocOffer:
		a.sendDocumentLink(chatID, "Публичная оферта", docKindOffer)
	case btnDocPrivacy:
		a.sendDocumentLink(chatID, "Политика конфиденциальности", docKindPrivacy)
	case btnDocRefund:
		a.sendDocumentLink(chatID, "Политика возврата", docKindRefund)
	case btnBack:
		state.Step = StepMainMenu
		_ = a.stateStore.Set(ctx, chatID, state)
		a.sendMainMenu(chatID, true)
	default:
		// Любое непонятное сообщение — возвращаем в главное меню.
		state.Step = StepMainMenu
		_ = a.stateStore.Set(ctx, chatID, state)
		a.sendMainMenu(chatID, true)
	}
}

// docKind — какой документ запрошен.
type docKind int

const (
	docKindUser docKind = iota
	docKindOffer
	docKindPrivacy
	docKindRefund
)

// sendDocumentLink отправляет пользователю ссылку на Telegraph-страницу документа.
// Если документы ещё не опубликованы (Telegraph недоступен на старте) — сообщает
// о временной недоступности.
func (a *App) sendDocumentLink(chatID int64, title string, kind docKind) {
	if a.documents == nil {
		a.sendText(chatID, documentsUnavailableText(), servicesMenuKeyboard())
		return
	}

	userURL, offerURL, privacyURL, refundURL, ready := a.documents.links()
	if !ready {
		a.sendText(chatID, documentsUnavailableText(), servicesMenuKeyboard())
		return
	}

	var url string
	switch kind {
	case docKindUser:
		url = userURL
	case docKindOffer:
		url = offerURL
	case docKindPrivacy:
		url = privacyURL
	case docKindRefund:
		url = refundURL
	}

	if url == "" {
		a.sendText(chatID, documentsUnavailableText(), servicesMenuKeyboard())
		return
	}

	msg := tgbotapi.NewMessage(chatID, "📄 *"+title+"*\n\n"+url)
	msg.ParseMode = "Markdown"
	msg.ReplyMarkup = servicesMenuKeyboard()
	if _, err := a.bot.Send(msg); err != nil {
		log.Printf("[tg-bot] failed to send document link: %v", err)
	}
}
