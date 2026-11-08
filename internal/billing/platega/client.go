// Package platega — HTTP-клиент Platega и чистые правила обработки платежей.
//
// Пакет не зависит ни от БД, ни от Kafka: он знает только контракт API и
// то, как статус транзакции превращается в решение о заказе. Всё, что
// касается хранения, транзакций и outbox, живёт в пакете billing.
//
// Контракт API взят из официальной документации https://docs.platega.io
// (проверено 2026-11-05; редакция документации от 2026-10-27):
//
//	Базовый адрес:  https://app.platega.io/
//	Авторизация:    заголовки X-MerchantId (UUID) и X-Secret (API-ключ)
//	Создание:       POST /transaction/process
//	Статус:         GET  /transaction/{id}
//	Ошибка:         {code, type, message, data, traceId}
//
// Идемпотентность создания транзакции в документации НЕ описана. Поэтому
// клиент различает «запрос точно не обработан» и «результат неизвестен»
// (RequestError.Ambiguous), а повтор создания вслепую запрещён на уровне
// сервиса.
package platega

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// DefaultBaseURL — базовый адрес API из документации.
const DefaultBaseURL = "https://app.platega.io"

// maxResponseBytes — предел чтения ответа API. Ответы создания и статуса
// занимают сотни байт; лимит защищает от неожиданно большого тела.
const maxResponseBytes = 64 << 10

// ErrNotFound — GET /transaction/{id} вернул 404.
var ErrNotFound = errors.New("platega: transaction not found")

// Config — параметры подключения. Секрет хранится только в памяти и
// никогда не попадает в логи и ошибки.
type Config struct {
	BaseURL       string
	MerchantID    string
	Secret        string
	PaymentMethod PaymentMethod
}

// Validate проверяет полноту и корректность конфигурации.
func (c Config) Validate() error {
	if strings.TrimSpace(c.MerchantID) == "" {
		return errors.New("platega: merchant id is empty")
	}
	if !IsUUID(strings.TrimSpace(c.MerchantID)) {
		return errors.New("platega: merchant id must be a UUID")
	}
	if strings.TrimSpace(c.Secret) == "" {
		return errors.New("platega: secret is empty")
	}
	if _, ok := KnownPaymentMethods[c.PaymentMethod]; !ok {
		return fmt.Errorf("platega: unsupported payment method %d", c.PaymentMethod)
	}
	u, err := url.Parse(strings.TrimSpace(c.BaseURL))
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") {
		return errors.New("platega: base url must be an absolute http(s) url")
	}
	return nil
}

// Client — клиент Platega API. Безопасен для конкурентного использования.
type Client struct {
	baseURL    string
	merchantID string
	secret     string
	method     PaymentMethod
	http       *http.Client
}

// NewClient создаёт клиент. httpClient обязан иметь конечный Timeout;
// если передан nil, используется клиент с таймаутом 15 секунд. Переходы по
// редиректам отключены: POST создания не должен молча превращаться в GET.
func NewClient(cfg Config, httpClient *http.Client) (*Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 15 * time.Second}
	}
	hc := *httpClient
	hc.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Client{
		baseURL:    strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/"),
		merchantID: strings.TrimSpace(cfg.MerchantID),
		secret:     cfg.Secret,
		method:     cfg.PaymentMethod,
		http:       &hc,
	}, nil
}

// PaymentMethod возвращает настроенный способ оплаты.
func (c *Client) PaymentMethod() PaymentMethod { return c.method }

// CreateRequest — данные новой транзакции. Сумма задаётся в копейках и
// берётся только из доверенного серверного тарифа.
type CreateRequest struct {
	AmountMinor int64
	Currency    string
	Description string
	ReturnURL   string
	FailedURL   string
	Payload     string
	OrderID     string
	UserEmail   string
	UserID      string
}

// CreateResult — результат успешного создания.
type CreateResult struct {
	TransactionID string
	RedirectURL   string
	Status        Status
	ExpiresIn     time.Duration // 0 — поле не пришло или не распознано
	// Raw — ответ API для аудита (секретов не содержит).
	Raw json.RawMessage
}

// Transaction — состояние транзакции по данным GET /transaction/{id}.
// Персональные поля ответа (person, contactPhoneNumber и т.п.) намеренно не
// разбираются и не сохраняются.
type Transaction struct {
	ID          string
	Status      Status
	AmountMinor int64
	Currency    string
	Payload     string
}

// RequestError — ошибка обращения к API.
type RequestError struct {
	Op string // create | status
	// Ambiguous=true: запрос мог быть обработан (таймаут после отправки,
	// 5xx, неразборчивый ответ 2xx). Повторять создание вслепую нельзя.
	Ambiguous  bool
	HTTPStatus int
	Code       string
	TraceID    string
	Message    string
	Err        error
}

func (e *RequestError) Error() string {
	var b strings.Builder
	b.WriteString("platega ")
	b.WriteString(e.Op)
	if e.HTTPStatus != 0 {
		b.WriteString(" http=")
		b.WriteString(strconv.Itoa(e.HTTPStatus))
	}
	if e.Code != "" {
		b.WriteString(" code=")
		b.WriteString(e.Code)
	}
	if e.TraceID != "" {
		b.WriteString(" trace_id=")
		b.WriteString(e.TraceID)
	}
	if e.Message != "" {
		b.WriteString(": ")
		b.WriteString(e.Message)
	}
	if e.Err != nil {
		b.WriteString(": ")
		b.WriteString(e.Err.Error())
	}
	if e.Ambiguous {
		b.WriteString(" (outcome unknown)")
	}
	return b.String()
}

func (e *RequestError) Unwrap() error { return e.Err }

// IsAmbiguous сообщает, что исход операции неизвестен.
func IsAmbiguous(err error) bool {
	var re *RequestError
	return errors.As(err, &re) && re.Ambiguous
}

type createBody struct {
	PaymentMethod  int            `json:"paymentMethod"`
	PaymentDetails paymentDetails `json:"paymentDetails"`
	Description    string         `json:"description"`
	Return         string         `json:"return"`
	FailedURL      string         `json:"failedUrl"`
	Payload        string         `json:"payload,omitempty"`
	OrderID        string         `json:"orderId,omitempty"`
	Metadata       createMetadata `json:"metadata"`
}

type paymentDetails struct {
	Amount   json.Number `json:"amount"`
	Currency string      `json:"currency"`
}

type createMetadata struct {
	UserEmail string `json:"user_email"`
	UserID    string `json:"userId,omitempty"`
}

type createResponse struct {
	TransactionID string `json:"transactionId"`
	Redirect      string `json:"redirect"`
	Status        string `json:"status"`
	ExpiresIn     string `json:"expiresIn"`
}

type statusResponse struct {
	ID             string `json:"id"`
	Status         string `json:"status"`
	PaymentDetails *struct {
		Amount   json.Number `json:"amount"`
		Currency string      `json:"currency"`
	} `json:"paymentDetails"`
	Payload string `json:"payload"`
}

type errorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	TraceID string `json:"traceId"`
}

// CreateTransaction создаёт транзакцию POST /transaction/process.
func (c *Client) CreateTransaction(ctx context.Context, req CreateRequest) (*CreateResult, error) {
	if req.AmountMinor <= 0 {
		return nil, &RequestError{Op: "create", Message: "amount must be positive"}
	}
	currency := strings.ToUpper(strings.TrimSpace(req.Currency))
	if currency == "" || strings.TrimSpace(req.UserEmail) == "" ||
		strings.TrimSpace(req.ReturnURL) == "" || strings.TrimSpace(req.FailedURL) == "" ||
		strings.TrimSpace(req.Description) == "" {
		return nil, &RequestError{Op: "create", Message: "required field is empty"}
	}
	body, err := json.Marshal(createBody{
		PaymentMethod: int(c.method),
		PaymentDetails: paymentDetails{
			Amount:   json.Number(FormatMinor(req.AmountMinor)),
			Currency: currency,
		},
		Description: req.Description,
		Return:      req.ReturnURL,
		FailedURL:   req.FailedURL,
		Payload:     req.Payload,
		OrderID:     req.OrderID,
		Metadata: createMetadata{
			UserEmail: req.UserEmail,
			UserID:    req.UserID,
		},
	})
	if err != nil {
		return nil, &RequestError{Op: "create", Err: err}
	}

	status, raw, sent, err := c.do(ctx, http.MethodPost, "/transaction/process", body)
	if err != nil {
		// Тело запроса не было записано целиком — сервер не мог его
		// обработать. Иначе (таймаут/обрыв после отправки) исход неизвестен.
		return nil, &RequestError{Op: "create", Ambiguous: sent, Err: err}
	}
	if status < 200 || status > 299 {
		e := apiError("create", status, raw)
		// 5xx и 3xx не говорят, создана ли транзакция; 4xx — отказ в приёме.
		e.Ambiguous = status >= 500 || (status >= 300 && status < 400)
		return nil, e
	}

	var parsed createResponse
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, &RequestError{Op: "create", Ambiguous: true, HTTPStatus: status,
			Message: "undecodable success response", Err: err}
	}
	if !IsUUID(strings.TrimSpace(parsed.TransactionID)) {
		return nil, &RequestError{Op: "create", Ambiguous: true, HTTPStatus: status,
			Message: "success response without valid transactionId"}
	}
	redirect := strings.TrimSpace(parsed.Redirect)
	if u, perr := url.Parse(redirect); perr != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") {
		return nil, &RequestError{Op: "create", Ambiguous: true, HTTPStatus: status,
			Message: "success response without valid redirect url"}
	}
	st := StatusPending
	if parsed.Status != "" {
		if s, perr := ParseStatus(parsed.Status); perr == nil {
			st = s
		}
	}
	res := &CreateResult{
		TransactionID: strings.ToLower(strings.TrimSpace(parsed.TransactionID)),
		RedirectURL:   redirect,
		Status:        st,
		Raw:           json.RawMessage(raw),
	}
	if d, perr := ParseTimeSpan(parsed.ExpiresIn); perr == nil {
		res.ExpiresIn = d
	}
	return res, nil
}

// GetTransaction запрашивает состояние транзакции GET /transaction/{id}.
func (c *Client) GetTransaction(ctx context.Context, id string) (*Transaction, error) {
	id = strings.TrimSpace(id)
	if !IsUUID(id) {
		return nil, &RequestError{Op: "status", Message: "transaction id is not a UUID"}
	}
	status, raw, _, err := c.do(ctx, http.MethodGet, "/transaction/"+url.PathEscape(id), nil)
	if err != nil {
		return nil, &RequestError{Op: "status", Err: err}
	}
	if status == http.StatusNotFound {
		e := apiError("status", status, raw)
		e.Err = ErrNotFound
		return nil, e
	}
	if status < 200 || status > 299 {
		return nil, apiError("status", status, raw)
	}
	var parsed statusResponse
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, &RequestError{Op: "status", HTTPStatus: status, Message: "undecodable response", Err: err}
	}
	st, err := ParseStatus(parsed.Status)
	if err != nil {
		return nil, &RequestError{Op: "status", HTTPStatus: status, Err: err}
	}
	if parsed.PaymentDetails == nil {
		return nil, &RequestError{Op: "status", HTTPStatus: status, Message: "response without paymentDetails"}
	}
	amount, err := ParseJSONAmountMinor(parsed.PaymentDetails.Amount)
	if err != nil {
		return nil, &RequestError{Op: "status", HTTPStatus: status, Err: err}
	}
	return &Transaction{
		ID:          strings.TrimSpace(parsed.ID),
		Status:      st,
		AmountMinor: amount,
		Currency:    strings.ToUpper(strings.TrimSpace(parsed.PaymentDetails.Currency)),
		Payload:     parsed.Payload,
	}, nil
}

// do выполняет запрос. sent=true означает, что запрос (заголовки и тело)
// был полностью записан в соединение — после этого исход POST неизвестен,
// если ответ не получен.
func (c *Client) do(ctx context.Context, method, path string, body []byte) (int, []byte, bool, error) {
	var wrote atomic.Bool
	trace := &httptrace.ClientTrace{
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			if info.Err == nil {
				wrote.Store(true)
			}
		},
	}
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(httptrace.WithClientTrace(ctx, trace), method, c.baseURL+path, rdr)
	if err != nil {
		return 0, nil, false, err
	}
	req.Header.Set("X-MerchantId", c.merchantID)
	req.Header.Set("X-Secret", c.secret)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return 0, nil, wrote.Load(), err
	}
	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		return 0, nil, true, fmt.Errorf("read response: %w", err)
	}
	if len(raw) > maxResponseBytes {
		return 0, nil, true, errors.New("response body too large")
	}
	return resp.StatusCode, raw, true, nil
}

func apiError(op string, status int, raw []byte) *RequestError {
	e := &RequestError{Op: op, HTTPStatus: status}
	var parsed errorResponse
	if json.Unmarshal(raw, &parsed) == nil {
		e.Code = truncate(parsed.Code, 64)
		e.TraceID = truncate(parsed.TraceID, 64)
		e.Message = truncate(parsed.Message, 200)
	}
	return e
}

func truncate(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) <= n {
		return s
	}
	return s[:n]
}
