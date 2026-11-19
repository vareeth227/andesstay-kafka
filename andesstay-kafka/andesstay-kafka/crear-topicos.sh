#!/bin/bash
# crear-topicos.sh
# Crea los DOS topicos reales del caso AndesStay con 3 particiones y RF 3,
# con su retencion y cleanup.policy. Ejecutar UNA vez tras levantar el cluster.
#
# Uso (desde la EC2, con el cluster Kafka arriba):
#   bash crear-topicos.sh
#
# Corre los comandos dentro del contenedor kafka-1.

set -e
BROKER="kafka-1:9092"

echo ">> Creando topico reservas-eventos (negocio)"
docker exec kafka-1 kafka-topics --bootstrap-server $BROKER \
  --create --if-not-exists \
  --topic reservas-eventos \
  --partitions 3 \
  --replication-factor 3 \
  --config retention.ms=604800000 \
  --config cleanup.policy=delete
# retention.ms=604800000  -> 7 dias. cleanup.policy=delete -> borra por tiempo.

echo ">> Creando topico reservas-auditoria (auditoria)"
docker exec kafka-1 kafka-topics --bootstrap-server $BROKER \
  --create --if-not-exists \
  --topic reservas-auditoria \
  --partitions 3 \
  --replication-factor 3 \
  --config retention.ms=1209600000 \
  --config cleanup.policy=delete
# retention.ms=1209600000 -> 14 dias (la auditoria se guarda mas tiempo).

echo ">> Topicos existentes:"
docker exec kafka-1 kafka-topics --bootstrap-server $BROKER --list

echo ">> Detalle de reservas-eventos:"
docker exec kafka-1 kafka-topics --bootstrap-server $BROKER --describe --topic reservas-eventos

echo ">> Detalle de reservas-auditoria:"
docker exec kafka-1 kafka-topics --bootstrap-server $BROKER --describe --topic reservas-auditoria

echo ">> Listo. Verifica en Kafka UI que cada topico tenga 3 particiones y 3 replicas."
