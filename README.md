# Implementación - Python

## Descripción

Implementación de dos wrappers sobre `pika` para exponer una interfaz simplificada de RabbitMQ:

- **MessageMiddlewareQueueRabbitMQ**: Work Queue
- **MessageMiddlewareExchangeRabbitMQ**: Exchange con routing por clave

## Patrones implementados

### Work Queue
Cola compartida donde múltiples consumidores distribuyen la carga. Cada mensaje es entregado a un único consumidor y requiere confirmación explícita.

### Exchange
Exchange tipo `direct` donde cada consumidor crea su propia cola exclusiva y la bindea a sus `routing_keys`. Un mensaje publicado se envía a todas las routing keys con las que fue inicializada la instancia.

## Decisiones de diseño

**Abstracción de callbacks:** los callbacks internos de `pika` se adaptan a la forma `(message, ack, nack)` para ocultar detalles de AMQP.

**ACK manual:** se deshabilita `auto_ack` para que la confirmación o rechazo de cada mensaje quede bajo control explícito del consumidor.

**Separación entre publicación y consumo en Exchange:** la cola exclusiva del consumidor se crea recién al comenzar a escuchar, evitando declarar colas y bindings innecesarios del lado publisher.

**Prefetch bajo:** `prefetch_count=1` prioriza reparto equilibrado entre consumidores en los tests de work queue.

**Publicación confiable:** los mensajes se publican con publisher confirms y `mandatory=True`, para no asumir silenciosamente que fueron aceptados y enroutados.

**Reconexión simple:** ante fallos de conexión durante publicación se realiza un reintento acotado con backoff corto y re-declaración de la topología.

## Invariantes y precondiciones

- `send()` en `Exchange` requiere al menos una `routing_key`.
- `start_consuming()` no debe invocarse dos veces en paralelo sobre la misma instancia.
- Luego de `close()`, la instancia no vuelve a ser utilizable.
- En `Exchange`, cada consumidor recibe mensajes a través de su propia cola exclusiva.

## Ejecución

```bash
make up
make logs
make down