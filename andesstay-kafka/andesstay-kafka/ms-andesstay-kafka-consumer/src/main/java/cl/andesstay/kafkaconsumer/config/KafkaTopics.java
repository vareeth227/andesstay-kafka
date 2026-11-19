package cl.andesstay.kafkaconsumer.config;

/**
 * Nombres de topicos y consumer groups de AndesStay, centralizados.
 * Deben coincidir EXACTAMENTE con los del productor y Kafka UI.
 */
public final class KafkaTopics {

    public static final String RESERVAS_EVENTOS = "reservas-eventos";
    public static final String RESERVAS_AUDITORIA = "reservas-auditoria";

    // Consumer groups (uno por topico, para verlos separados en el lag).
    public static final String GROUP_EVENTOS = "andesstay-reservas-eventos-group";
    public static final String GROUP_AUDITORIA = "andesstay-reservas-auditoria-group";

    private KafkaTopics() {
    }
}
