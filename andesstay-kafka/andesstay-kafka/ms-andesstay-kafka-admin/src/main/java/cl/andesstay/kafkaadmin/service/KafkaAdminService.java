package cl.andesstay.kafkaadmin.service;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Logica de administracion de topicos usando el AdminClient de Kafka.
 * Cada metodo valida la entrada y traduce el resultado a estructuras simples
 * (Map / List) que el controller devuelve como JSON.
 */
@Service
public class KafkaAdminService {

    private static final Logger log = LoggerFactory.getLogger(KafkaAdminService.class);

    private final AdminClient adminClient;

    public KafkaAdminService(AdminClient adminClient) {
        this.adminClient = adminClient;
    }

    /** Lista los nombres de todos los topicos del cluster. */
    public List<String> listTopics() throws Exception {
        List<String> topics = new ArrayList<>(adminClient.listTopics().names().get());
        Collections.sort(topics);
        return topics;
    }

    /** Describe un topico: particiones, lider, replicas e ISR. */
    public Map<String, Object> describeTopic(String name) throws Exception {
        validarNombre(name);
        TopicDescription desc = adminClient.describeTopics(Collections.singletonList(name))
                .allTopicNames().get().get(name);

        List<Map<String, Object>> particiones = new ArrayList<>();
        for (TopicPartitionInfo p : desc.partitions()) {
            Map<String, Object> info = new LinkedHashMap<>();
            info.put("particion", p.partition());
            info.put("lider", p.leader() != null ? p.leader().id() : null);
            info.put("replicas", p.replicas().stream().map(n -> n.id()).toList());
            info.put("isr", p.isr().stream().map(n -> n.id()).toList());
            particiones.add(info);
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("topico", desc.name());
        result.put("numeroParticiones", desc.partitions().size());
        result.put("factorReplicacion", desc.partitions().get(0).replicas().size());
        result.put("particiones", particiones);
        return result;
    }

    /** Crea un topico con el nombre, particiones y factor de replicacion dados. */
    public Map<String, Object> createTopic(String name, int partitions, short replicationFactor) throws Exception {
        validarNombre(name);
        if (partitions < 1) {
            throw new IllegalArgumentException("particiones debe ser mayor o igual a 1");
        }
        if (replicationFactor < 1) {
            throw new IllegalArgumentException("replicationFactor debe ser mayor o igual a 1");
        }

        NewTopic nuevo = new NewTopic(name, partitions, replicationFactor);
        adminClient.createTopics(Collections.singletonList(nuevo)).all().get();
        log.info("Topico creado -> nombre: {} particiones: {} replicacion: {}",
                name, partitions, replicationFactor);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("topico", name);
        result.put("particiones", partitions);
        result.put("factorReplicacion", replicationFactor);
        result.put("creado", true);
        return result;
    }

    /** Consulta las configuraciones del topico (retention.ms, cleanup.policy, etc.). */
    public Map<String, String> getTopicConfig(String name) throws Exception {
        validarNombre(name);
        ConfigResource recurso = new ConfigResource(ConfigResource.Type.TOPIC, name);
        Config config = adminClient.describeConfigs(Collections.singletonList(recurso))
                .all().get().get(recurso);

        Map<String, String> result = new LinkedHashMap<>();
        for (ConfigEntry entry : config.entries()) {
            result.put(entry.name(), entry.value());
        }
        return result;
    }

    /** Modifica una configuracion del topico (ej. retention.ms o cleanup.policy). */
    public Map<String, Object> updateTopicConfig(String name, String key, String value) throws Exception {
        validarNombre(name);
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("key es obligatorio");
        }
        ConfigResource recurso = new ConfigResource(ConfigResource.Type.TOPIC, name);
        AlterConfigOp op = new AlterConfigOp(new ConfigEntry(key, value), AlterConfigOp.OpType.SET);
        adminClient.incrementalAlterConfigs(
                Collections.singletonMap(recurso, Collections.singletonList(op))).all().get();
        log.info("Config modificada -> topico: {} {}={}", name, key, value);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("topico", name);
        result.put(key, value);
        result.put("actualizado", true);
        return result;
    }

    /** Borra un topico (usar solo con topicos temporales de prueba). */
    public Map<String, Object> deleteTopic(String name) throws Exception {
        validarNombre(name);
        adminClient.deleteTopics(Collections.singletonList(name)).all().get();
        log.info("Topico borrado -> {}", name);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("topico", name);
        result.put("borrado", true);
        return result;
    }

    /** Validacion: el nombre del topico no puede venir vacio. */
    private void validarNombre(String name) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("nombre del topico es obligatorio");
        }
    }
}
