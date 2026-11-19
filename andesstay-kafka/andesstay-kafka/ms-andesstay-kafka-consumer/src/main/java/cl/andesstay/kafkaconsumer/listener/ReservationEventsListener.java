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
 * Consumidor del topico de NEGOCIO (reservas-eventos).
 * Procesa los hechos funcionales: reserva creada, confirmada, cancelada.
 *
 * Aqui iria la logica de reporteria / integracion. En esta version se
 * registra el procesamiento (el consumidor no tiene base de datos propia).
 */
@Component
public class ReservationEventsListener {

    private static final Logger log = LoggerFactory.getLogger(ReservationEventsListener.class);

    @KafkaListener(
            topics = KafkaTopics.RESERVAS_EVENTOS,
            groupId = KafkaTopics.GROUP_EVENTOS)
    public void onBusinessEvent(
            @Payload ReservationKafkaEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        log.info("Evento de negocio recibido -> topic: {} particion: {} offset: {} eventId: {} tipo: {}",
                KafkaTopics.RESERVAS_EVENTOS, partition, offset, event.getEventId(), event.getEventType());

        // 1. Validar antes de procesar (la guia lo exige).
        validar(event);

        // 2. Procesar segun el tipo de evento.
        switch (event.getEventType()) {
            case "RESERVA_CREADA":
                log.info("Procesando RESERVA_CREADA reservaId: {} huesped: {}",
                        event.getReservationId(), event.getGuestName());
                break;
            case "RESERVA_CONFIRMADA":
                log.info("Procesando RESERVA_CONFIRMADA reservaId: {} estado: {}",
                        event.getReservationId(), event.getStatus());
                break;
            case "RESERVA_CANCELADA":
                log.info("Procesando RESERVA_CANCELADA reservaId: {}", event.getReservationId());
                break;
            default:
                log.warn("Tipo de evento no reconocido: {} (reservaId: {})",
                        event.getEventType(), event.getReservationId());
        }

        log.info("Procesamiento OK -> eventId: {} reservaId: {}",
                event.getEventId(), event.getReservationId());
    }

    /** Validacion minima: si falta un dato clave, lanza error (ira a reintento). */
    private void validar(ReservationKafkaEvent event) {
        if (event.getReservationId() == null || event.getEventType() == null) {
            throw new IllegalArgumentException(
                    "Evento invalido: falta reservationId o eventType (eventId: "
                            + event.getEventId() + ")");
        }
    }
}
