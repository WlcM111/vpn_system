package tg_bot_gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// ============================================================================
// Лёгкий клиент Telegra.ph (без внешних зависимостей, только stdlib).
//
// Назначение: публиковать длинные документы (Пользовательское соглашение,
// Публичная оферта, Политика конфиденциальности) как Telegraph-страницы и
// отдавать пользователю ссылки. В сообщении Telegram длинный текст не помещается
// (лимит 4096 символов), а Telegraph-страница открывается мгновенно (Instant View).
//
// Поток: createAccount (один раз, получаем access_token) → createPage для каждого
// документа → сохраняем URL. Делается при старте бота; URL кэшируются в памяти.
// ============================================================================

const telegraphAPIBase = "https://api.telegra.ph"

// telegraphClient — клиент Telegraph с access_token аккаунта.
type telegraphClient struct {
	http        *http.Client
	accessToken string
}

// telegraphNode — узел контента Telegraph. Может быть строкой (текст) или
// элементом с тегом. Для наших документов используем простые параграфы.
type telegraphNode struct {
	Tag      string           `json:"tag,omitempty"`
	Children []telegraphChild `json:"children,omitempty"`
}

// telegraphChild — ребёнок узла: либо текст (string), либо вложенный узел.
// Используем json.RawMessage-подход через интерфейс, но для простоты —
// строки достаточно для текстовых документов.
type telegraphChild struct {
	text string
	node *telegraphNode
}

// MarshalJSON: строковый ребёнок сериализуется как JSON-строка, узловой — как объект.
func (c telegraphChild) MarshalJSON() ([]byte, error) {
	if c.node != nil {
		return json.Marshal(c.node)
	}
	return json.Marshal(c.text)
}

// telegraphAPIResponse — обёртка ответа Telegraph API.
type telegraphAPIResponse struct {
	OK     bool            `json:"ok"`
	Error  string          `json:"error,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
}

// telegraphAccount — результат createAccount.
type telegraphAccount struct {
	AccessToken string `json:"access_token"`
	AuthURL     string `json:"auth_url,omitempty"`
}

// telegraphPage — результат createPage.
type telegraphPage struct {
	Path string `json:"path"`
	URL  string `json:"url"`
}

// newTelegraphClient создаёт клиент с заданным HTTP-таймаутом.
// Запросы к Telegraph идут через тот же прокси, что и к Telegram API
// (переменные окружения HTTPS_PROXY / TG_HTTPS_PROXY): центральный сервер
// не имеет прямого доступа к api.telegra.ph, но имеет его через прокси-ноду.
func newTelegraphClient() *telegraphClient {
	transport := &http.Transport{
		Proxy: telegraphProxyFunc(),
	}
	return &telegraphClient{
		http: &http.Client{
			Timeout:   15 * time.Second,
			Transport: transport,
		},
	}
}

// telegraphProxyFunc возвращает функцию выбора прокси для http.Transport.
// Берёт URL прокси из TG_HTTPS_PROXY (приоритетно) либо HTTPS_PROXY/HTTP_PROXY.
// Если прокси не задан — возвращает nil (прямое соединение).
func telegraphProxyFunc() func(*http.Request) (*url.URL, error) {
	raw := strings.TrimSpace(os.Getenv("TG_HTTPS_PROXY"))
	if raw == "" {
		raw = strings.TrimSpace(os.Getenv("HTTPS_PROXY"))
	}
	if raw == "" {
		raw = strings.TrimSpace(os.Getenv("HTTP_PROXY"))
	}
	if raw == "" {
		return nil
	}
	proxyURL, err := url.Parse(raw)
	if err != nil {
		return nil
	}
	return http.ProxyURL(proxyURL)
}

// createAccount создаёт аккаунт Telegraph и сохраняет access_token в клиенте.
// shortName — техническое имя (не видно читателям), authorName — подпись под заголовком.
func (c *telegraphClient) createAccount(ctx context.Context, shortName, authorName string) error {
	form := map[string]string{
		"short_name":  shortName,
		"author_name": authorName,
	}
	var acc telegraphAccount
	if err := c.call(ctx, "createAccount", form, &acc); err != nil {
		return err
	}
	if acc.AccessToken == "" {
		return fmt.Errorf("telegraph createAccount returned empty access_token")
	}
	c.accessToken = acc.AccessToken
	return nil
}

// createPage публикует страницу из обычного текста и возвращает её URL.
// Текст разбивается на абзацы по пустым строкам; каждый абзац — параграф <p>.
func (c *telegraphClient) createPage(ctx context.Context, title, authorName, plainText string) (string, error) {
	if c.accessToken == "" {
		return "", fmt.Errorf("telegraph: no access token (call createAccount first)")
	}

	content := plainTextToNodes(plainText)
	contentJSON, err := json.Marshal(content)
	if err != nil {
		return "", fmt.Errorf("telegraph marshal content: %w", err)
	}

	form := map[string]string{
		"access_token": c.accessToken,
		"title":        title,
		"author_name":  authorName,
		"content":      string(contentJSON),
	}
	var page telegraphPage
	if err := c.call(ctx, "createPage", form, &page); err != nil {
		return "", err
	}
	if page.URL == "" {
		return "", fmt.Errorf("telegraph createPage returned empty url")
	}
	return page.URL, nil
}

// call выполняет POST-запрос к Telegraph API с form-urlencoded телом.
func (c *telegraphClient) call(ctx context.Context, method string, form map[string]string, out any) error {
	var body bytes.Buffer
	first := true
	for k, v := range form {
		if !first {
			body.WriteByte('&')
		}
		first = false
		body.WriteString(urlEncode(k))
		body.WriteByte('=')
		body.WriteString(urlEncode(v))
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, telegraphAPIBase+"/"+method, &body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("telegraph %s http: %w", method, err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}

	var parsed telegraphAPIResponse
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return fmt.Errorf("telegraph %s decode: %w (body=%s)", method, err, string(raw))
	}
	if !parsed.OK {
		return fmt.Errorf("telegraph %s failed: %s", method, parsed.Error)
	}
	if out != nil && len(parsed.Result) > 0 {
		if err := json.Unmarshal(parsed.Result, out); err != nil {
			return fmt.Errorf("telegraph %s decode result: %w", method, err)
		}
	}
	return nil
}

// plainTextToNodes превращает обычный текст в массив Telegraph-узлов.
// Каждый непустой абзац (разделённый \n\n) становится параграфом <p>.
// Одиночные переносы внутри абзаца сохраняются как <br>.
func plainTextToNodes(text string) []telegraphNode {
	text = strings.ReplaceAll(text, "\r\n", "\n")
	paragraphs := strings.Split(text, "\n\n")

	nodes := make([]telegraphNode, 0, len(paragraphs))
	for _, p := range paragraphs {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}

		// внутри абзаца одиночные \n превращаем в <br/>
		lines := strings.Split(p, "\n")
		children := make([]telegraphChild, 0, len(lines)*2)
		for i, line := range lines {
			if i > 0 {
				children = append(children, telegraphChild{node: &telegraphNode{Tag: "br"}})
			}
			children = append(children, telegraphChild{text: line})
		}

		nodes = append(nodes, telegraphNode{Tag: "p", Children: children})
	}

	if len(nodes) == 0 {
		// Telegraph требует непустой контент
		nodes = append(nodes, telegraphNode{Tag: "p", Children: []telegraphChild{{text: " "}}})
	}
	return nodes
}

// urlEncode — минимальное percent-encoding для form-urlencoded значений.
func urlEncode(s string) string {
	var b strings.Builder
	for _, r := range []byte(s) {
		switch {
		case (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') ||
			r == '-' || r == '_' || r == '.' || r == '~':
			b.WriteByte(r)
		case r == ' ':
			b.WriteByte('+')
		default:
			b.WriteString(fmt.Sprintf("%%%02X", r))
		}
	}
	return b.String()
}
