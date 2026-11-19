package cl.andesstay.kafkaadmin.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

/**
 * Crea el AdminClient de Kafka (el cliente de administracion del cluster).
 * Lee la direccion de los brokers desde application.yml.
 */
@Configuration
public class AdminClientConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean(destroyMethod = "close")
    public AdminClient adminClient() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return AdminClient.create(props);
    }
}
