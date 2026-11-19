package cl.andesstay.kafkaconsumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Microservicio consumidor Kafka de AndesStay.
 * Escucha los topicos reservas-eventos y reservas-auditoria y procesa los
 * eventos publicados por el microservicio de reservas.
 */
@SpringBootApplication
public class KafkaConsumerApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaConsumerApplication.class, args);
    }
}
