package cl.andesstay.kafkaadmin;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Microservicio administrador de Kafka de AndesStay.
 * Expone una API REST para crear, listar, consultar, modificar y borrar
 * topicos, usando el AdminClient de Kafka.
 */
@SpringBootApplication
public class KafkaAdminApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaAdminApplication.class, args);
    }
}
