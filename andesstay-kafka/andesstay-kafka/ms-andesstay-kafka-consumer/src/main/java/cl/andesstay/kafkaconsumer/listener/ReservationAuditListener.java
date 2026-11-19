package cl.andesstay.kafkaconsumer.listener;

import cl.andesstay.kafkaconsumer.config.KafkaTopics;
import cl.andesstay.kafkaconsumer.event.ReservationKafkaEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Consumidor del topico de AUDITORIA (reservas-auditoria).
 * Registra la trazabilidad de las acciones: deja constancia de cada cambio
 * de estado de las reservas para fines de auditoria.
 *
 * Usa un consumer group DISTINTO al de negocio, para poder revisar su lag
 * por separado en Kafka UI / CLI.
 */
@Component
public class ReservationAuditListener {

    private static final Logger log = LoggerFactory.getLogger(ReservationAuditListener.class);

    @KafkaListener(
            topics = KafkaTopics.RESERVAS_AUDITORIA,
            groupId = KafkaTopics.GROUP_AUDITORIA)
    public void onAuditEvent(
            @Payload ReservationKafkaEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        log.info("Evento de auditoria recibido -> topic: {} particion: {} offset: {} eventId: {} tipo: {}",
                KafkaTopics.RESERVAS_AUDITORIA, partition, offset, event.getEventId(), event.getEventType());

        if (event.getReservationId() == null) {
            throw new IllegalArgumentException(
                    "Evento de auditoria invalido: falta reservationId (eventId: "
                            + event.getEventId() + ")");
        }

        // Registro de trazabilidad (quien, que y cuando).
        log.info("AUDITORIA -> reservaId: {} accion: {} estado: {} ocurrio: {} correlationId: {}",
                event.getReservationId(), event.getEventType(), event.getStatus(),
                event.getOccurredAt(), event.getCorrelationId());
    }
}
