# Admin Kafka - como probarlo (curl) y demo de la presentacion

Base: http://<IP-o-localhost>:8086/api/kafka/admin

## Listar topicos
curl http://localhost:8086/api/kafka/admin/topics

## Describir un topico (particiones, lider, replicas, ISR)
curl http://localhost:8086/api/kafka/admin/topics/reservas-eventos

## Ver configuracion de un topico (retention.ms, cleanup.policy, etc.)
curl http://localhost:8086/api/kafka/admin/topics/reservas-eventos/config

## Crear un topico TEMPORAL de prueba (201)
curl -X POST "http://localhost:8086/api/kafka/admin/topics?name=topico-prueba&partitions=3&replicationFactor=3"

## Crear con parametros invalidos (400) - para demostrar la validacion
curl -X POST "http://localhost:8086/api/kafka/admin/topics?name=&partitions=3&replicationFactor=3"
# -> 400 {"error":"Parametro invalido","detail":"nombre del topico es obligatorio"}

## Modificar una configuracion (ej. cambiar retencion del topico de prueba)
curl -X PUT "http://localhost:8086/api/kafka/admin/topics/topico-prueba/config?key=retention.ms&value=3600000"

## Borrar el topico TEMPORAL de prueba (NUNCA los dos reales)
curl -X DELETE http://localhost:8086/api/kafka/admin/topics/topico-prueba

## IMPORTANTE para la defensa (CP-02 / CP-06)
- Los dos topicos reales (reservas-eventos, reservas-auditoria) se crean con
  crear-topicos.sh, NO se borran en la demo.
- Para demostrar crear/borrar se usa SIEMPRE topico-prueba.
- Mostrar que un parametro invalido devuelve 400 sin tocar Kafka.
