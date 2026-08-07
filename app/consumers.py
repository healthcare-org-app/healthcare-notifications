"""Kafka consumers for notifications-service.

One handler per subscribed topic. Handlers are best-effort logging plus
audit — services override this file to implement real cross-domain behavior.
"""
from __future__ import annotations

import logging

from healthcare_common.audit import emit_audit

log = logging.getLogger("notifications-service.consumers")


def register(svc) -> None:
    bus = svc.bus

    @bus.on("appointment.booked")
    def _on_appointment_booked(envelope: dict) -> None:
        log.info("notifications-service: received appointment.booked id=%s", envelope.get("id"))
        emit_audit(bus, action="consume.appointment.booked", actor="system:notifications-service",
                   target=None, details={"envelope_id": envelope.get("id")})

    @bus.on("appointment.cancelled")
    def _on_appointment_cancelled(envelope: dict) -> None:
        log.info("notifications-service: received appointment.cancelled id=%s", envelope.get("id"))
        emit_audit(bus, action="consume.appointment.cancelled", actor="system:notifications-service",
                   target=None, details={"envelope_id": envelope.get("id")})

    @bus.on("lab.result.available")
    def _on_lab_result_available(envelope: dict) -> None:
        log.info("notifications-service: received lab.result.available id=%s", envelope.get("id"))
        emit_audit(bus, action="consume.lab.result.available", actor="system:notifications-service",
                   target=None, details={"envelope_id": envelope.get("id")})

    @bus.on("imaging.result.available")
    def _on_imaging_result_available(envelope: dict) -> None:
        log.info("notifications-service: received imaging.result.available id=%s", envelope.get("id"))
        emit_audit(bus, action="consume.imaging.result.available", actor="system:notifications-service",
                   target=None, details={"envelope_id": envelope.get("id")})

    @bus.on("prescription.issued")
    def _on_prescription_issued(envelope: dict) -> None:
        log.info("notifications-service: received prescription.issued id=%s", envelope.get("id"))
        emit_audit(bus, action="consume.prescription.issued", actor="system:notifications-service",
                   target=None, details={"envelope_id": envelope.get("id")})

    @bus.on("prescription.refill_requested")
    def _on_prescription_refill_requested(envelope: dict) -> None:
        log.info("notifications-service: received prescription.refill_requested id=%s", envelope.get("id"))
        emit_audit(bus, action="consume.prescription.refill_requested", actor="system:notifications-service",
                   target=None, details={"envelope_id": envelope.get("id")})

    @bus.on("invoice.issued")
    def _on_invoice_issued(envelope: dict) -> None:
        log.info("notifications-service: received invoice.issued id=%s", envelope.get("id"))
        emit_audit(bus, action="consume.invoice.issued", actor="system:notifications-service",
                   target=None, details={"envelope_id": envelope.get("id")})

    @bus.on("invoice.paid")
    def _on_invoice_paid(envelope: dict) -> None:
        log.info("notifications-service: received invoice.paid id=%s", envelope.get("id"))
        emit_audit(bus, action="consume.invoice.paid", actor="system:notifications-service",
                   target=None, details={"envelope_id": envelope.get("id")})

    @bus.on("device.alert.triggered")
    def _on_device_alert_triggered(envelope: dict) -> None:
        log.info("notifications-service: received device.alert.triggered id=%s", envelope.get("id"))
        emit_audit(bus, action="consume.device.alert.triggered", actor="system:notifications-service",
                   target=None, details={"envelope_id": envelope.get("id")})

