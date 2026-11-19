# Arbol de carpetas - donde va cada archivo

IMPORTANTE: cada archivo .java debe quedar en la ruta que corresponde a su
"package". Si el package dice cl.andesstay.kafkaconsumer.listener, el archivo
va en src/main/java/cl/andesstay/kafkaconsumer/listener/.

====================================================================
A) ms-andesstay-kafka-consumer   (repo o carpeta nueva)
====================================================================
ms-andesstay-kafka-consumer/
├── pom.xml
├── Dockerfile
└── src/
    └── main/
        ├── java/
        │   └── cl/andesstay/kafkaconsumer/
        │       ├── KafkaConsumerApplication.java
        │       ├── config/
        │       │   ├── KafkaTopics.java
        │       │   └── KafkaConsumerConfig.java
        │       ├── event/
        │       │   └── ReservationKafkaEvent.java
        │       └── listener/
        │           ├── ReservationEventsListener.java
        │           └── ReservationAuditListener.java
        └── resources/
            └── application.yml

====================================================================
B) ms-andesstay-kafka-admin   (repo o carpeta nueva)
====================================================================
ms-andesstay-kafka-admin/
├── pom.xml
├── Dockerfile
└── src/
    └── main/
        ├── java/
        │   └── cl/andesstay/kafkaadmin/
        │       ├── KafkaAdminApplication.java
        │       ├── config/
        │       │   └── AdminClientConfig.java
        │       ├── service/
        │       │   └── KafkaAdminService.java
        │       └── web/
        │           └── KafkaAdminController.java
        └── resources/
            └── application.yml

====================================================================
C) ms-andesstay-reservations   (microservicio YA existente)
====================================================================
Agregar SOLO estos 3 archivos nuevos (no se borra nada):
src/main/java/cl/andesstay/reservations/kafka/
├── KafkaTopics.java
├── ReservationKafkaEvent.java
└── ReservationKafkaPublisher.java

Ademas:
- pom.xml de reservations: agregar la dependencia spring-kafka.
- application.yml de reservations: pegar el bloque spring.kafka.
- El servicio de reservas: llamar al publisher en crear/confirmar/cancelar
  (ver INTEGRACION_reservations.md).

====================================================================
D) Infra (donde esta el docker-compose de EP2)
====================================================================
- docker-compose.kafka.yml        (cluster Kafka: 3 ZK + 3 brokers + UI)
- docker-compose.kafka-apps.yml   (consumer + admin)
- crear-topicos.sh                (crea los 2 topicos reales)
