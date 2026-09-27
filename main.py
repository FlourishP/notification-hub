"""Notification Hub — Multi-channel notification service.

Email, SMS, push notifications with templates and delivery tracking.
Python, FastAPI, Celery, Redis.
"""

import os
import logging
from datetime import datetime, timedelta
from typing import Optional
from enum import Enum

import redis
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from celery import Celery

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Notification Hub", version="1.0.0")

# --- Redis ---
redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    db=0,
    decode_responses=True,
)

# --- Celery ---
celery_app = Celery(
    "notifications",
    broker=os.getenv("CELERY_BROKER", "redis://localhost:6379/0"),
    backend=os.getenv("CELERY_BACKEND", "redis://localhost:6379/1"),
)


# --- Enums ---
class Channel(str, Enum):
    EMAIL = "email"
    SMS = "sms"
    PUSH = "push"


class Status(str, Enum):
    PENDING = "pending"
    SENT = "sent"
    FAILED = "failed"
    DELIVERED = "delivered"


# --- Models ---
class NotificationRequest(BaseModel):
    recipient: str
    channel: Channel
    template_id: str
    subject: Optional[str] = None
    body: Optional[str] = None
    priority: int = Field(default=5, ge=1, le=10)
    metadata: Optional[dict] = None


class NotificationResponse(BaseModel):
    notification_id: str
    status: str
    channel: str
    sent_at: str


class DeliveryStatus(BaseModel):
    notification_id: str
    status: Status
    channel: Channel
    recipient: str
    attempts: int
    last_attempt: Optional[str]
    delivered_at: Optional[str]
    error: Optional[str]


# --- Templates ---
TEMPLATES = {
    "welcome": {
        "subject": "Welcome to our platform!",
        "body": "Hi there! Welcome aboard. We're excited to have you.",
    },
    "password_reset": {
        "subject": "Password Reset Request",
        "body": "Click the link below to reset your password.",
    },
    "order_confirmation": {
        "subject": "Order Confirmed",
        "body": "Your order has been confirmed and is being processed.",
    },
    "alert": {
        "subject": "System Alert",
        "body": "An alert has been triggered. Please review.",
    },
}


# --- Celery tasks ---
@celery_app.task(bind=True, max_retries=3)
def send_email_task(self, recipient: str, subject: str, body: str):
    """Send email notification."""
    try:
        # Simulate email sending
        logger.info(f"Sending email to {recipient}: {subject}")
        # Track delivery
        redis_client.setex(
            f"notif:{self.request.id}",
            3600,
            str({"status": "sent", "channel": "email", "recipient": recipient}),
        )
        return {"status": "sent", "notification_id": self.request.id}
    except Exception as exc:
        logger.error(f"Email send failed: {exc}")
        raise self.retry(exc=exc, countdown=60)


@celery_app.task(bind=True, max_retries=3)
def send_sms_task(self, recipient: str, body: str):
    """Send SMS notification."""
    try:
        logger.info(f"Sending SMS to {recipient}: {body[:50]}")
        redis_client.setex(
            f"notif:{self.request.id}",
            3600,
            str({"status": "sent", "channel": "sms", "recipient": recipient}),
        )
        return {"status": "sent", "notification_id": self.request.id}
    except Exception as exc:
        logger.error(f"SMS send failed: {exc}")
        raise self.retry(exc=exc, countdown=60)


@celery_app.task(bind=True, max_retries=3)
def send_push_task(self, recipient: str, title: str, body: str):
    """Send push notification."""
    try:
        logger.info(f"Sending push to {recipient}: {title}")
        redis_client.setex(
            f"notif:{self.request.id}",
            3600,
            str({"status": "sent", "channel": "push", "recipient": recipient}),
        )
        return {"status": "sent", "notification_id": self.request.id}
    except Exception as exc:
        logger.error(f"Push send failed: {exc}")
        raise self.retry(exc=exc, countdown=60)


# --- API ---
@app.get("/health")
def health():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}


@app.post("/notify", response_model=NotificationResponse)
def send_notification(req: NotificationRequest):
    """Send a notification via the specified channel."""
    import uuid

    notif_id = str(uuid.uuid4())
    template = TEMPLATES.get(req.template_id, {"subject": req.subject or "", "body": req.body or ""})
    subject = req.subject or template["subject"]
    body = req.body or template["body"]

    # Queue the appropriate task
    if req.channel == Channel.EMAIL:
        task = send_email_task.apply_async(
            args=[req.recipient, subject, body],
            headers={"notification_id": notif_id},
        )
    elif req.channel == Channel.SMS:
        task = send_sms_task.apply_async(
            args=[req.recipient, body],
            headers={"notification_id": notif_id},
        )
    elif req.channel == Channel.PUSH:
        task = send_push_task.apply_async(
            args=[req.recipient, subject, body],
            headers={"notification_id": notif_id},
        )
    else:
        raise HTTPException(status_code=400, detail=f"Unsupported channel: {req.channel}")

    # Store notification metadata
    redis_client.setex(
        f"notif_meta:{notif_id}",
        86400,
        str({
            "notification_id": notif_id,
            "recipient": req.recipient,
            "channel": req.channel,
            "template_id": req.template_id,
            "task_id": task.id,
            "created_at": datetime.utcnow().isoformat(),
            "priority": req.priority,
        }),
    )

    return NotificationResponse(
        notification_id=notif_id,
        status="pending",
        channel=req.channel,
        sent_at=datetime.utcnow().isoformat(),
    )


@app.get("/status/{notification_id}")
def get_status(notification_id: str):
    """Get delivery status of a notification."""
    result = redis_client.get(f"notif:{notification_id}")
    meta = redis_client.get(f"notif_meta:{notification_id}")
    if not result:
        raise HTTPException(status_code=404, detail="Notification not found")
    return {"notification_id": notification_id, "result": result, "metadata": meta}


@app.get("/templates")
def list_templates():
    """List available notification templates."""
    return {"templates": list(TEMPLATES.keys())}


@app.get("/stats")
def get_stats():
    """Get notification statistics."""
    keys = redis_client.keys("notif:*")
    return {"total_notifications": len(keys), "redis_status": redis_client.ping()}