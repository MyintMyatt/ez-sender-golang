package models

import (
	"errors"
	"time"
)

type Channel string

const (
	ChannelEmail Channel = "email"
	ChannelSMS   Channel = "sms"
	ChannelPush  Channel = "push"
)

type NotificationPriority uint8

const (
	LowPriority    NotificationPriority = 1
	NormalPriority NotificationPriority = 2
	HighPriority   NotificationPriority = 4
	UrgentPriority NotificationPriority = 5
)

type Locale string

const (
	LocaleEN Locale = "en"
	LocaleMM Locale = "mm"
)

type NotificationRequest struct {
	ID         string                 `json:"id"`
	Recipient  string                 `json:"recipient"` // user id or email
	Priority   NotificationPriority   `json:"priority"`
	Channel    Channel                `json:"channel"`
	ClientTime time.Time              `json:"client_time"`
	Template   string                 `json:"template"`
	Locale     Locale                 `json:"locale"`
	Subject    string                 `json:"subject"`
	Data       map[string]interface{} `json:"data,omitempty"`
}

func (n *NotificationRequest) Validate() error {
	if n.ID == "" {
		return errors.New("missing notification id")
	}

	if n.Subject == "" {
		return errors.New("email subject can't be null")
	}

	if n.Priority == 0 {
		n.Priority = NormalPriority
	}
	return nil
}
