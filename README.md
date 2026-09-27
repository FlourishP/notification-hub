# Notification Hub

Multi-channel notification service: email, SMS, push notifications with templates and delivery tracking.

## Features
- **Multi-Channel**: Email (SendGrid), SMS (Twilio), Push (FCM)
- **Template Engine**: Dynamic template rendering with variables
- **Delivery Tracking**: Open rates, click rates, delivery status
- **Queue Processing**: Celery worker-based async delivery
- **A/B Testing**: Template variant testing and analytics
- **Preferences**: Per-user notification preference management

## Tech Stack
- **Backend**: Python, FastAPI, Celery
- **Queue**: Redis
- **Database**: PostgreSQL
- **Email**: SendGrid
- **SMS**: Twilio

## Quick Start
```bash
git clone https://github.com/FlourishP/notification-hub
cd notification-hub
docker-compose up -d
pip install -r requirements.txt
celery -A worker worker --loglevel=info
```

## License
MIT — see [LICENSE](LICENSE)
