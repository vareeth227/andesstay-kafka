# Como enganchar el productor Kafka al flujo de reservas (para Sergio)

## 1. Dependencia (pom.xml de ms-andesstay-reservations)
Agregar dentro de <dependencies>:

    <dependency>
        <groupId>org.springframework.kafka</groupId>
        <artifactId>spring-kafka</artifactId>
    </dependency>

(La version la maneja el spring-boot-starter-parent; no hace falta indicarla.)

## 2. Archivos nuevos (paquete cl.andesstay.reservations.kafka)
- KafkaTopics.java
- ReservationKafkaEvent.java
- ReservationKafkaPublisher.java

## 3. application.yml
Pegar el bloque spring.kafka del archivo application-kafka-reservations.yml.

## 4. Publicar en el flujo real (ReservationService)
En el servicio que ya crea/confirma/cancela reservas (el mismo que hoy publica
en RabbitMQ), inyectar el publisher y publicar DESPUES de guardar en Oracle.

    private final ReservationKafkaPublisher kafkaPublisher;
    // ...constructor con el publisher...

    // Ejemplo al confirmar una reserva:
    public Reservation confirm(Long id) {
        Reservation r = /* ...logica existente que cambia el estado a CONFIRMADA y guarda... */;

        // Evento de negocio -> reservas-eventos
        ReservationKafkaEvent evento = new ReservationKafkaEvent(
                "evt-" + java.util.UUID.randomUUID(),      // eventId
                "RESERVA_CONFIRMADA",                       // eventType
                java.time.Instant.now(),                    // occurredAt
                "res-" + r.getId(),                         // correlationId
                r.getId(), r.getUnitId(), r.getGuestName(), r.getStatus());
        kafkaPublisher.publishBusinessEvent(evento);

        // Evento de auditoria -> reservas-auditoria (mismo hecho, para trazabilidad)
        kafkaPublisher.publishAuditEvent(evento);

        return r;
    }

Repetir el patron en crear (RESERVA_CREADA) y cancelar (RESERVA_CANCELADA).

## Nota
- NO se toca ni se quita nada de RabbitMQ: Kafka se suma, convive.
- El productor Kafka es SOLO este microservicio (regla de productor unico).
- El catalogo NO publica en Kafka.
