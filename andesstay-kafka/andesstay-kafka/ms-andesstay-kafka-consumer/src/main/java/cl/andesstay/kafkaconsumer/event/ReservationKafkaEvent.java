package cl.andesstay.kafkaconsumer.event;

import java.time.Instant;

/**
 * DTO del evento recibido desde Kafka. Debe tener la MISMA estructura que el
 * del productor (ms-andesstay-reservations) para que el JSON deserialice bien.
 */
public class ReservationKafkaEvent {

    private String eventId;
    private String eventType;
    private Instant occurredAt;
    private String correlationId;
    private Long reservationId;
    private Long unitId;
    private String guestName;
    private String status;

    public ReservationKafkaEvent() {
    }

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
