# Implementación - Python

## Descripción

Implementación de dos clases wrapper sobre `pika` para simplificar la comunicación con RabbitMQ:

- **MessageMiddlewareQueueRabbitMQ**: Work Queue (distribución de trabajo)
- **MessageMiddlewareExchangeRabbitMQ**: Pub/Sub con routing (broadcast selectivo)

## Patrones Implementados

### Work Queue
Cola compartida donde múltiples consumidores distribuyen la carga. Cada mensaje es procesado por un único consumidor.


### Pub/Sub (Exchange)
Exchange tipo `topic` donde cada consumidor tiene su propia cola temporal. Un mensaje puede llegar a múltiples consumidores según patrones de ruteo.

## Decisiones de Diseño

**Abstracción de callbacks:** Los callbacks de `pika` (4 parámetros) se simplifican a `(message, ack, nack)`. El usuario recibe el mensaje y funciones para confirmar/rechazar sin conocer detalles del protocolo AMQP.

**ACK manual obligatorio:** Se deshabilitó ACK automático para permitir control explícito del procesamiento y evitar pérdida de mensajes ante fallos.

**Colas exclusivas en Exchange:** Cada consumidor crea su propia cola temporal que se elimina automáticamente al desconectar. Garantiza que todos los interesados reciban el mensaje (broadcast).

**QoS balanceado:** `prefetch_count=1` asegura que los consumidores rápidos procesen más mensajes que los lentos.

## Ejecución
```bash
# Ejecutar tests con Docker
make up

# Ver logs
make logs

# Detener
make down
```

**Resultado esperado:** 19/19 tests pasados (13 Queue + 6 Exchange).