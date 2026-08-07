# notifications-service

notifications-service — domain: notifications

- **Port:** 9000
- **Language:** Python 3.11 + Flask
- **Database:** `notifications` (Postgres, table `notifications`)
- **Event bus:** Kafka

## API

| Method    | Path                       |
|-----------|----------------------------|
| GET       | `/api/notifications/`          |
| POST      | `/api/notifications/`          |
| GET       | `/api/notifications/<id>`      |
| PUT/PATCH | `/api/notifications/<id>`      |
| DELETE    | `/api/notifications/<id>`      |
| GET       | `/health`                  |
| GET       | `/ready`                   |

## Events

**Publishes:** notification.requested
**Subscribes:** appointment.booked, appointment.cancelled, lab.result.available, imaging.result.available, prescription.issued, prescription.refill_requested, invoice.issued, invoice.paid, device.alert.triggered

## HTTP peer dependencies

- `patient-communications-service`
- `patients-service`
- `audit-log-service`

## Local dev

```bash
pip install -e ../../libs/py-healthcare-common
pip install -r requirements.txt
cp .env.example .env
(cd ../../infra && docker compose up -d postgres kafka kafka-init)
python -m app.main
```

## Tests

```bash
pytest
```
