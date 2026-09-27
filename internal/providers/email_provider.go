package providers

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/MyintMyatt/notification-service/internal/models"
	"github.com/mailjet/mailjet-apiv3-go/v4"
)

type EmailProvider struct {
	publicKey  string
	privateKey string
	fromEmail  string
	fromName   string
}

func NewEmailProvider(apikey string, secretkey string, fromMail string, fromName string) *EmailProvider {
	return &EmailProvider{
		publicKey:  apikey,
		privateKey: secretkey,
		fromEmail:  fromMail,
		fromName:   fromName,
	}
}

func (e *EmailProvider) Channel() models.Channel {
	return models.ChannelEmail
}

func (e *EmailProvider) SendNotification(ctx context.Context, request *models.NotificationRequest) error {
	slog.Info("[-]:Sending mail.....", "recipient", request.Recipient)
	mjClient := mailjet.NewMailjetClient(e.publicKey, e.privateKey)
	var templateID int
	var err error

	if request.Template != "" {
		templateID, err = strconv.Atoi(request.Template)
		if err != nil {
			return fmt.Errorf("invalid email template ID %q: %w", request.Template, err)
		}
	}

	msgInfo := []mailjet.InfoMessagesV31 {
      {
        From: &mailjet.RecipientV31{
          Email: e.fromEmail,
          Name: e.fromName,
        },
        To: &mailjet.RecipientsV31{
          mailjet.RecipientV31 {
            Email: request.Recipient,
            Name: "Customer",
          },
        },
        TemplateID: templateID,
        TemplateLanguage: true,
		Variables: request.Data,
        Subject: request.Subject,
      },
    }

	messages := mailjet.MessagesV31{Info: msgInfo}
	res, err := mjClient.SendMailV31(&messages)
	if err != nil {
		slog.Error(err.Error())
		return err
	}

	fmt.Printf("Data: %+v\n", res)
	slog.Info("[OK]:Sending mail success.")
	return nil
}
