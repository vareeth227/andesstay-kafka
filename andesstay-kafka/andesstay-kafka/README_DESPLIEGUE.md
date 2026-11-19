# Despliegue en EC2 - EP5 Kafka (puntos 3 y 4)

Orden para levantar Kafka y los microservicios en la EC2, junto a RabbitMQ.
Pegar los comandos de a uno (la terminal web duplica el pegado).

## PASO 0 - Subir archivos a la EC2
Dejar en ~/andesstay (junto al compose de EP2):
- docker-compose.kafka.yml
- docker-compose.kafka-apps.yml
- crear-topicos.sh
- las carpetas ms-andesstay-kafka-consumer/ y ms-andesstay-kafka-admin/
  (con su Dockerfile ya renombrado dentro de cada una)

## PASO 1 - Levantar el cluster Kafka (3 ZK + 3 brokers + UI)
    docker compose -f docker-compose.kafka.yml up -d

Esperar ~1 a 2 minutos. Verificar que los 7 contenedores esten Up:
    docker compose -f docker-compose.kafka.yml ps

Revisar que no haya errores de arranque:
    docker compose -f docker-compose.kafka.yml logs --tail=30 kafka-1

## PASO 2 - Crear los dos topicos reales
    bash crear-topicos.sh

Debe terminar mostrando reservas-eventos y reservas-auditoria, cada uno con
3 particiones y 3 replicas. Confirmar en Kafka UI (http://<IP>:8080).

## PASO 3 - Levantar consumer + admin
    docker compose -f docker-compose.kafka-apps.yml up -d --build

Verificar:
    docker compose -f docker-compose.kafka-apps.yml ps
    docker logs --tail=20 ms-andesstay-kafka-consumer
    docker logs --tail=20 ms-andesstay-kafka-admin

Probar el admin:
    curl http://localhost:8086/api/kafka/admin/topics

## PASO 4 - Reconstruir reservations (productor) con Kafka
Reservations necesita:
  a) los 3 archivos nuevos en cl.andesstay.reservations.kafka
  b) la dependencia spring-kafka en su pom.xml
  c) el bloque spring.kafka en su application.yml
  d) las llamadas al publisher en crear/confirmar/cancelar
  e) poder conectarse a los brokers por la red kafka-net

En el docker-compose de los microservicios (el de EP2 que levanta reservations),
agregar al servicio de reservations:

    environment:
      SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka-1:9092,kafka-2:9092,kafka-3:9092
    networks:
      - <red-actual-de-los-microservicios>
      - kafka-net

y al final del archivo, declarar la red externa:

    networks:
      kafka-net:
        external: true
        name: kafka-net

Luego reconstruir solo reservations (ajustar al nombre de archivo/servicio real):
    docker compose -f docker-compose.core.yml up -d --build ms-andesstay-reservations

## COMPROBACION RAPIDA (fin del punto 4)
- docker ps  -> estan arriba: 3 ZK, 3 kafka, kafka-ui, consumer, admin,
  reservations, y todo lo de RabbitMQ (2 nodos, notify, rabbit-admin, bff,
  catalog, frontend, caddy).
- Kafka UI muestra los 2 topicos con 3 particiones y 3 replicas.
- El admin responde en :8086.

A partir de aqui siguen los puntos 5 (pruebas y evidencias) y 6 (GitHub),
que los hace Sergio con el documento que le vamos a dejar.

## NOTA DE MEMORIA (EC2)
Si Kafka no levanta o se cae, casi siempre es falta de RAM. Revisar:
    free -h
    docker stats --no-stream
Si la instancia queda corta, hay que subir el tipo de instancia o levantar
Kafka solo para la demo (apagando lo que no se este mostrando en ese momento).
