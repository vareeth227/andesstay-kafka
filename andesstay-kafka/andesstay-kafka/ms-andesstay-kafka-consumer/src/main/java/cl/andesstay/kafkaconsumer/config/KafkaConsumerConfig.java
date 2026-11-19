package cl.andesstay.kafkaconsumer.config;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Configuracion del manejo de errores del consumidor.
 *
 * Estrategia (lo que pide la guia EP5):
 *  - Reintentos ACOTADOS: 2 reintentos con 1 segundo de espera.
 *  - Si tras los reintentos sigue fallando, se registra el error y se
 *    DESCARTA ese mensaje para no bloquear la particion indefinidamente.
 *  - Un mensaje corrupto (que no deserializa) no tumba al consumidor,
 *    gracias al ErrorHandlingDeserializer configurado en application.yml.
 *
 * (Ampliacion opcional para Sergio: en vez de descartar, reenviar el mensaje
 *  fallido a un topico de error tipo "reservas-eventos-dlt" con un
 *  DeadLetterPublishingRecoverer. Aqui se deja el manejo basico requerido.)
 */
@Configuration
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    @Bean
    public DefaultErrorHandler kafkaErrorHandler() {
        // 1000 ms entre intentos, maximo 2 reintentos (3 intentos en total).
        FixedBackOff backOff = new FixedBackOff(1000L, 2L);

        DefaultErrorHandler handler = new DefaultErrorHandler((record, exception) -> {
            // Se ejecuta cuando se agotan los reintentos: se registra y se sigue.
            ConsumerRecord<?, ?> rec = record;
            log.error("ERROR Kafka: mensaje descartado tras reintentos -> topic: {} particion: {} offset: {} causa: {}",
                    rec.topic(), rec.partition(), rec.offset(), exception.getMessage());
        }, backOff);

        return handler;
    }
}
