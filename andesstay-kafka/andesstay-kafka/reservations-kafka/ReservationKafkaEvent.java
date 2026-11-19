package cl.andesstay.reservations.kafka;

import java.time.Instant;

/**
 * DTO del evento que viaja por Kafka (se serializa como JSON).
 *
 * Misma estructura para el topico de negocio y el de auditoria. Lleva los
 * datos minimos para que el consumidor identifique QUE paso y pueda procesar
 * el caso de uso sin volver a consultar la base de datos.
 *
 * Ejemplo serializado:
 * {
 *   "eventId": "evt-1a2b3c",
 *   "eventType": "RESERVA_CONFIRMADA",
 *   "occurredAt": "2026-11-18T23:30:00Z",
 *   "correlationId": "res-21",
 *   "reservationId": 21,
 *   "unitId": 1,
 *   "guestName": "Benjamin Pena",
 *   "status": "CONFIRMADA"
 * }
 */
public class ReservationKafkaEvent {

    /** Identificador unico de este evento (numero de serie del mensaje). */
    private String eventId;

    /** Tipo de accion: RESERVA_CREADA, RESERVA_CONFIRMADA, RESERVA_CANCELADA, etc. */
    private String eventType;

    /** Momento en que ocurrio el hecho. */
    private Instant occurredAt;

    /** Agrupa todos los eventos de una misma reserva (ej. "res-21"). */
    private String correlationId;

    /** Id de la reserva afectada. Se usa tambien como key de Kafka. */
    private Long reservationId;

    /** Unidad / habitacion reservada. */
    private Long unitId;

    /** Nombre del huesped. */
    private String guestName;

    /** Estado de la reserva tras la accion (CREADA, CONFIRMADA, CANCELADA...). */
    private String status;

    // Constructor vacio: lo necesita el deserializador JSON.
    public ReservationKafkaEvent() {
    }

    public ReservationKafkaEvent(String eventId, String eventType, Instant occurredAt,
                                 String correlationId, Long reservationId, Long unitId,
                                 String guestName, String status) {
        this.eventId = eventId;
        this.eventType = eventType;
        this.occurredAt = occurredAt;
        this.correlationId = correlationId;
        this.reservationId = reservationId;
        this.unitId = unitId;
        this.guestName = guestName;
        this.status = status;
    }

    // Getters y setters (los necesita Jackson para serializar/deserializar).
    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }

    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }

    public Instant getOccurredAt() { return occurredAt; }
    public void setOccurredAt(Instant occurredAt) { this.occurredAt = occurredAt; }

    public String getCorrelationId() { return correlationId; }
    public void setCorrelationId(String correlationId) { this.correlationId = correlationId; }

    public Long getReservationId() { return reservationId; }
    public void setReservationId(Long reservationId) { this.reservationId = reservationId; }

    public Long getUnitId() { return unitId; }
    public void setUnitId(Long unitId) { this.unitId = unitId; }

    public String getGuestName() { return guestName; }
    public void setGuestName(String guestName) { this.guestName = guestName; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
}
