package cl.andesstay.reservations.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * Productor Kafka de AndesStay (unico productor del caso, dentro del
 * microservicio principal de reservas).
 *
 * Publica en los dos topicos:
 *   - reservas-eventos    -> hechos de negocio (crear, confirmar, cancelar).
 *   - reservas-auditoria  -> registro de trazabilidad de esas acciones.
 *
 * Usa el reservationId como KEY del mensaje: asi todos los eventos de una
 * misma reserva caen en la misma particion y conservan el orden entre ellos.
 */
@Service
public class ReservationKafkaPublisher {

    private static final Logger log = LoggerFactory.getLogger(ReservationKafkaPublisher.class);

    // KafkaTemplate lo crea Spring Boot a partir de la config en application.yml.
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public ReservationKafkaPublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /** Publica un evento de negocio en el topico reservas-eventos. */
    public void publishBusinessEvent(ReservationKafkaEvent event) {
        publish(KafkaTopics.RESERVAS_EVENTOS, event);
    }

    /** Publica un evento de auditoria en el topico reservas-auditoria. */
    public void publishAuditEvent(ReservationKafkaEvent event) {
        publish(KafkaTopics.RESERVAS_AUDITORIA, event);
    }

    /**
     * Envia el evento al topico indicado y registra el resultado en el log.
     * El envio es asincrono: no bloquea la operacion de negocio.
     */
    private void publish(String topic, ReservationKafkaEvent event) {
        String key = event.getReservationId() != null
                ? String.valueOf(event.getReservationId())
                : event.getEventId();

        log.info("Publicando evento Kafka -> topic: {} key: {} eventId: {} eventType: {}",
                topic, key, event.getEventId(), event.getEventType());

        kafkaTemplate.send(topic, key, event).whenComplete((result, ex) -> {
            if (ex != null) {
                // El negocio ya se persistio: un fallo de publicacion se registra,
                // no se propaga al cliente (misma politica que el productor RabbitMQ).
                log.error("ERROR al publicar evento Kafka -> topic: {} eventId: {}: {}",
                        topic, event.getEventId(), ex.getMessage(), ex);
            } else {
                log.info("Evento Kafka publicado OK -> topic: {} particion: {} offset: {} eventId: {}",
                        topic,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset(),
                        event.getEventId());
            }
        });
    }
}
