package cl.andesstay.reservations.kafka;

/**
 * Nombres de los topicos Kafka del caso AndesStay, centralizados en un
 * unico lugar (igual que RabbitMQTopology para RabbitMQ en EV2).
 *
 * Asi el productor, el consumidor y el administrador usan exactamente el
 * mismo nombre y no hay strings sueltos repartidos por el codigo.
 */
public final class KafkaTopics {

    /** Topico de eventos de negocio: creacion, confirmacion, cancelacion de reservas. */
    public static final String RESERVAS_EVENTOS = "reservas-eventos";

    /** Topico de auditoria: trazabilidad de acciones y cambios de estado. */
    public static final String RESERVAS_AUDITORIA = "reservas-auditoria";

    // Clase de solo constantes: no se instancia.
    private KafkaTopics() {
    }
}
