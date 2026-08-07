"""Kafka consumers for notifications-service.

One handler per subscribed topic. Real handlers write to this service's own
database and/or publish follow-up events; stub handlers just log + audit.
"""
from __future__ import annotations

import logging

from psycopg.types.json import Json

from healthcare_common.audit import emit_audit

log = logging.getLogger("notifications-service.consumers")

TABLE = "notifications"


def register(svc) -> None:
    bus = svc.bus
    db = svc.db
    clients = svc.clients

    @bus.on("appointment.booked")
    def _on_appointment_booked(envelope: dict) -> None:
        data = envelope.get("data") or {}
        try:
                    # This service is a fan-out point. Turn the incoming domain event into a
                    # `notification.requested` so gateway services can deliver it.
                    subject_map = {{
                        "lab.result.available":     "Lab result available",
                        "imaging.result.available": "Imaging result available",
                        "appointment.booked":       "Appointment confirmed",
                        "appointment.cancelled":    "Appointment cancelled",
                        "prescription.issued":      "Prescription ready",
                        "prescription.refill_requested": "Refill received",
                        "invoice.issued":           "New invoice",
                        "invoice.paid":             "Payment received",
                        "device.alert.triggered":   "Device alert",
                    }}
                    subject = subject_map.get(envelope.get("event_type"), "Health update")
                    bus.publish("notification.requested",
                                key=str(data.get("patient_id") or data.get("id") or ""),
                                value={{"patient_id": data.get("patient_id"),
                                       "subject":    subject,
                                       "source":     envelope.get("event_type"),
                                       "payload":    data}})
        except Exception as e:
            log.exception("notifications-service/appointment.booked handler failed: %s", e)
        emit_audit(bus, action="consume.appointment.booked", actor="system:notifications-service",
                   target=None, details={"envelope_id": envelope.get("id")})

    @bus.on("appointment.cancelled")
    def _on_appointment_cancelled(envelope: dict) -> None:
        data = envelope.get("data") or {}
        try:
                    # This service is a fan-out point. Turn the incoming domain event into a
                    # `notification.requested` so gateway services can deliver it.
                    subject_map = {{
                        "lab.result.available":     "Lab result available",
                        "imaging.result.available": "Imaging result available",
                        "appointment.booked":       "Appointment confirmed",
                        "appointment.cancelled":    "Appointment cancelled",
                        "prescription.issued":      "Prescription ready",
                        "prescription.refill_requested": "Refill received",
                        "invoice.issued":           "New invoice",
                        "invoice.paid":             "Payment received",
                        "device.alert.triggered":   "Device alert",
                    }}
                    subject = subject_map.get(envelope.get("event_type"), "Health update")
                    bus.publish("notification.requested",
                                key=str(data.get("patient_id") or data.get("id") or ""),
                                value={{"patient_id": data.get("patient_id"),
                                       "subject":    subject,
                                       "source":     envelope.get("event_type"),
                                       "payload":    data}})
        except Exception as e:
            log.exception("notifications-service/appointment.cancelled handler failed: %s", e)
        emit_audit(bus, action="consume.appointment.cancelled", actor="system:notifications-service",
                   target=None, details={"envelope_id": envelope.get("id")})

    @bus.on("lab.result.available")
    def _on_lab_result_available(envelope: dict) -> None:
        data = envelope.get("data") or {}
        try:
                    # This service is a fan-out point. Turn the incoming domain event into a
                    # `notification.requested` so gateway services can deliver it.
                    subject_map = {{
                        "lab.result.available":     "Lab result available",
                        "imaging.result.available": "Imaging result available",
                        "appointment.booked":       "Appointment confirmed",
                        "appointment.cancelled":    "Appointment cancelled",
                        "prescription.issued":      "Prescription ready",
                        "prescription.refill_requested": "Refill received",
                        "invoice.issued":           "New invoice",
                        "invoice.paid":             "Payment received",
                        "device.alert.triggered":   "Device alert",
                    }}
                    subject = subject_map.get(envelope.get("event_type"), "Health update")
                    bus.publish("notification.requested",
                                key=str(data.get("patient_id") or data.get("id") or ""),
                                value={{"patient_id": data.get("patient_id"),
                                       "subject":    subject,
                                       "source":     envelope.get("event_type"),
                                       "payload":    data}})
        except Exception as e:
            log.exception("notifications-service/lab.result.available handler failed: %s", e)
        emit_audit(bus, action="consume.lab.result.available", actor="system:notifications-service",
                   target=None, details={"envelope_id": envelope.get("id")})

    @bus.on("imaging.result.available")
    def _on_imaging_result_available(envelope: dict) -> None:
        data = envelope.get("data") or {}
        try:
                    # This service is a fan-out point. Turn the incoming domain event into a
                    # `notification.requested` so gateway services can deliver it.
                    subject_map = {{
                        "lab.result.available":     "Lab result available",
                        "imaging.result.available": "Imaging result available",
                        "appointment.booked":       "Appointment confirmed",
                        "appointment.cancelled":    "Appointment cancelled",
                        "prescription.issued":      "Prescription ready",
                        "prescription.refill_requested": "Refill received",
                        "invoice.issued":           "New invoice",
                        "invoice.paid":             "Payment received",
                        "device.alert.triggered":   "Device alert",
                    }}
                    subject = subject_map.get(envelope.get("event_type"), "Health update")
                    bus.publish("notification.requested",
                                key=str(data.get("patient_id") or data.get("id") or ""),
                                value={{"patient_id": data.get("patient_id"),
                                       "subject":    subject,
                                       "source":     envelope.get("event_type"),
                                       "payload":    data}})
        except Exception as e:
            log.exception("notifications-service/imaging.result.available handler failed: %s", e)
        emit_audit(bus, action="consume.imaging.result.available", actor="system:notifications-service",
                   target=None, details={"envelope_id": envelope.get("id")})

    @bus.on("prescription.issued")
    def _on_prescription_issued(envelope: dict) -> None:
        data = envelope.get("data") or {}
        try:
                    # This service is a fan-out point. Turn the incoming domain event into a
                    # `notification.requested` so gateway services can deliver it.
                    subject_map = {{
                        "lab.result.available":     "Lab result available",
                        "imaging.result.available": "Imaging result available",
                        "appointment.booked":       "Appointment confirmed",
                        "appointment.cancelled":    "Appointment cancelled",
                        "prescription.issued":      "Prescription ready",
                        "prescription.refill_requested": "Refill received",
                        "invoice.issued":           "New invoice",
                        "invoice.paid":             "Payment received",
                        "device.alert.triggered":   "Device alert",
                    }}
                    subject = subject_map.get(envelope.get("event_type"), "Health update")
                    bus.publish("notification.requested",
                                key=str(data.get("patient_id") or data.get("id") or ""),
                                value={{"patient_id": data.get("patient_id"),
                                       "subject":    subject,
                                       "source":     envelope.get("event_type"),
                                       "payload":    data}})
        except Exception as e:
            log.exception("notifications-service/prescription.issued handler failed: %s", e)
        emit_audit(bus, action="consume.prescription.issued", actor="system:notifications-service",
                   target=None, details={"envelope_id": envelope.get("id")})

    @bus.on("prescription.refill_requested")
    def _on_prescription_refill_requested(envelope: dict) -> None:
        data = envelope.get("data") or {}
        try:
                    # This service is a fan-out point. Turn the incoming domain event into a
                    # `notification.requested` so gateway services can deliver it.
                    subject_map = {{
                        "lab.result.available":     "Lab result available",
                        "imaging.result.available": "Imaging result available",
                        "appointment.booked":       "Appointment confirmed",
                        "appointment.cancelled":    "Appointment cancelled",
                        "prescription.issued":      "Prescription ready",
                        "prescription.refill_requested": "Refill received",
                        "invoice.issued":           "New invoice",
                        "invoice.paid":             "Payment received",
                        "device.alert.triggered":   "Device alert",
                    }}
                    subject = subject_map.get(envelope.get("event_type"), "Health update")
                    bus.publish("notification.requested",
                                key=str(data.get("patient_id") or data.get("id") or ""),
                                value={{"patient_id": data.get("patient_id"),
                                       "subject":    subject,
                                       "source":     envelope.get("event_type"),
                                       "payload":    data}})
        except Exception as e:
            log.exception("notifications-service/prescription.refill_requested handler failed: %s", e)
        emit_audit(bus, action="consume.prescription.refill_requested", actor="system:notifications-service",
                   target=None, details={"envelope_id": envelope.get("id")})

    @bus.on("invoice.issued")
    def _on_invoice_issued(envelope: dict) -> None:
        data = envelope.get("data") or {}
        try:
                    # This service is a fan-out point. Turn the incoming domain event into a
                    # `notification.requested` so gateway services can deliver it.
                    subject_map = {{
                        "lab.result.available":     "Lab result available",
                        "imaging.result.available": "Imaging result available",
                        "appointment.booked":       "Appointment confirmed",
                        "appointment.cancelled":    "Appointment cancelled",
                        "prescription.issued":      "Prescription ready",
                        "prescription.refill_requested": "Refill received",
                        "invoice.issued":           "New invoice",
                        "invoice.paid":             "Payment received",
                        "device.alert.triggered":   "Device alert",
                    }}
                    subject = subject_map.get(envelope.get("event_type"), "Health update")
                    bus.publish("notification.requested",
                                key=str(data.get("patient_id") or data.get("id") or ""),
                                value={{"patient_id": data.get("patient_id"),
                                       "subject":    subject,
                                       "source":     envelope.get("event_type"),
                                       "payload":    data}})
        except Exception as e:
            log.exception("notifications-service/invoice.issued handler failed: %s", e)
        emit_audit(bus, action="consume.invoice.issued", actor="system:notifications-service",
                   target=None, details={"envelope_id": envelope.get("id")})

    @bus.on("invoice.paid")
    def _on_invoice_paid(envelope: dict) -> None:
        data = envelope.get("data") or {}
        try:
                    # This service is a fan-out point. Turn the incoming domain event into a
                    # `notification.requested` so gateway services can deliver it.
                    subject_map = {{
                        "lab.result.available":     "Lab result available",
                        "imaging.result.available": "Imaging result available",
                        "appointment.booked":       "Appointment confirmed",
                        "appointment.cancelled":    "Appointment cancelled",
                        "prescription.issued":      "Prescription ready",
                        "prescription.refill_requested": "Refill received",
                        "invoice.issued":           "New invoice",
                        "invoice.paid":             "Payment received",
                        "device.alert.triggered":   "Device alert",
                    }}
                    subject = subject_map.get(envelope.get("event_type"), "Health update")
                    bus.publish("notification.requested",
                                key=str(data.get("patient_id") or data.get("id") or ""),
                                value={{"patient_id": data.get("patient_id"),
                                       "subject":    subject,
                                       "source":     envelope.get("event_type"),
                                       "payload":    data}})
        except Exception as e:
            log.exception("notifications-service/invoice.paid handler failed: %s", e)
        emit_audit(bus, action="consume.invoice.paid", actor="system:notifications-service",
                   target=None, details={"envelope_id": envelope.get("id")})

    @bus.on("device.alert.triggered")
    def _on_device_alert_triggered(envelope: dict) -> None:
        data = envelope.get("data") or {}
        try:
                    # This service is a fan-out point. Turn the incoming domain event into a
                    # `notification.requested` so gateway services can deliver it.
                    subject_map = {{
                        "lab.result.available":     "Lab result available",
                        "imaging.result.available": "Imaging result available",
                        "appointment.booked":       "Appointment confirmed",
                        "appointment.cancelled":    "Appointment cancelled",
                        "prescription.issued":      "Prescription ready",
                        "prescription.refill_requested": "Refill received",
                        "invoice.issued":           "New invoice",
                        "invoice.paid":             "Payment received",
                        "device.alert.triggered":   "Device alert",
                    }}
                    subject = subject_map.get(envelope.get("event_type"), "Health update")
                    bus.publish("notification.requested",
                                key=str(data.get("patient_id") or data.get("id") or ""),
                                value={{"patient_id": data.get("patient_id"),
                                       "subject":    subject,
                                       "source":     envelope.get("event_type"),
                                       "payload":    data}})
        except Exception as e:
            log.exception("notifications-service/device.alert.triggered handler failed: %s", e)
        emit_audit(bus, action="consume.device.alert.triggered", actor="system:notifications-service",
                   target=None, details={"envelope_id": envelope.get("id")})

