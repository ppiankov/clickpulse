package alerter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

// Alert represents a single alert event.
type Alert struct {
	Name    string
	Message string
}

// Alerter sends alerts via Telegram and/or webhook with cooldown deduplication.
type Alerter struct {
	telegramToken string
	telegramChat  string
	webhookURL    string
	cooldown      time.Duration

	mu       sync.Mutex
	lastSent map[string]time.Time
}

// New creates an alerter. Pass empty strings to disable a channel.
func New(telegramToken, telegramChat, webhookURL string, cooldown time.Duration) *Alerter {
	return &Alerter{
		telegramToken: telegramToken,
		telegramChat:  telegramChat,
		webhookURL:    webhookURL,
		cooldown:      cooldown,
		lastSent:      make(map[string]time.Time),
	}
}

// Enabled returns true if at least one alert channel is configured.
func (a *Alerter) Enabled() bool {
	return (a.telegramToken != "" && a.telegramChat != "") || a.webhookURL != ""
}

// Fire sends an alert if the cooldown for this alert name has expired.
func (a *Alerter) Fire(ctx context.Context, alert Alert) {
	if !a.Enabled() {
		return
	}

	a.mu.Lock()
	if last, ok := a.lastSent[alert.Name]; ok && time.Since(last) < a.cooldown {
		a.mu.Unlock()
		return
	}
	a.lastSent[alert.Name] = time.Now()
	a.mu.Unlock()

	if a.telegramToken != "" && a.telegramChat != "" {
		if err := a.sendTelegram(ctx, alert.Message); err != nil {
			log.Printf("telegram alert failed: %v", err)
		}
	}

	if a.webhookURL != "" {
		if err := a.sendWebhook(ctx, alert); err != nil {
			log.Printf("webhook alert failed: %v", err)
		}
	}
}

func (a *Alerter) sendTelegram(ctx context.Context, text string) error {
	url := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", a.telegramToken)
	payload := map[string]string{
		"chat_id": a.telegramChat,
		"text":    text,
	}
	body, _ := json.Marshal(payload)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("telegram returned %d", resp.StatusCode)
	}
	return nil
}

func (a *Alerter) sendWebhook(ctx context.Context, alert Alert) error {
	payload := map[string]string{
		"name":    alert.Name,
		"message": alert.Message,
	}
	body, _ := json.Marshal(payload)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.webhookURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("webhook returned %d", resp.StatusCode)
	}
	return nil
}
