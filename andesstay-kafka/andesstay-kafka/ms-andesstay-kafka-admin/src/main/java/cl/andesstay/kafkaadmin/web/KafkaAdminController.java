package cl.andesstay.kafkaadmin.web;

import cl.andesstay.kafkaadmin.service.KafkaAdminService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * API REST del administrador Kafka.
 * Base: /api/kafka/admin
 *
 * Operaciones:
 *   GET    /topics                      -> listar topicos
 *   GET    /topics/{nombre}             -> describir (particiones, replicas, ISR)
 *   POST   /topics?name=&partitions=&replicationFactor=  -> crear (201)
 *   GET    /topics/{nombre}/config      -> ver configuraciones
 *   PUT    /topics/{nombre}/config?key=&value=  -> modificar config
 *   DELETE /topics/{nombre}             -> borrar (solo topicos de prueba)
 *
 * Los parametros invalidos devuelven HTTP 400 con un mensaje claro.
 */
@RestController
@RequestMapping("/api/kafka/admin")
public class KafkaAdminController {

    private final KafkaAdminService service;

    public KafkaAdminController(KafkaAdminService service) {
        this.service = service;
    }

    @GetMapping("/topics")
    public List<String> listTopics() throws Exception {
        return service.listTopics();
    }

    @GetMapping("/topics/{nombre}")
    public Map<String, Object> describeTopic(@PathVariable String nombre) throws Exception {
        return service.describeTopic(nombre);
    }

    @PostMapping("/topics")
    public ResponseEntity<Map<String, Object>> createTopic(
            @RequestParam String name,
            @RequestParam(defaultValue = "3") int partitions,
            @RequestParam(defaultValue = "3") short replicationFactor) throws Exception {
        Map<String, Object> result = service.createTopic(name, partitions, replicationFactor);
        return ResponseEntity.status(HttpStatus.CREATED).body(result);
    }

    @GetMapping("/topics/{nombre}/config")
    public Map<String, String> getTopicConfig(@PathVariable String nombre) throws Exception {
        return service.getTopicConfig(nombre);
    }

    @PutMapping("/topics/{nombre}/config")
    public Map<String, Object> updateTopicConfig(
            @PathVariable String nombre,
            @RequestParam String key,
            @RequestParam String value) throws Exception {
        return service.updateTopicConfig(nombre, key, value);
    }

    @DeleteMapping("/topics/{nombre}")
    public Map<String, Object> deleteTopic(@PathVariable String nombre) throws Exception {
        return service.deleteTopic(nombre);
    }

    /**
     * Traduce los parametros invalidos a HTTP 400 con mensaje claro.
     * (La validacion ocurre ANTES de tocar Kafka.)
     */
    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> handleInvalid(IllegalArgumentException ex) {
        return ResponseEntity.badRequest().body(Map.of(
                "error", "Parametro invalido",
                "detail", ex.getMessage()));
    }
}
