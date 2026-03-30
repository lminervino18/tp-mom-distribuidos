import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):
    """
    Cola de mensajes usando RabbitMQ.
    """
    
    def __init__(self, host, queue_name):
        """
        Inicializa la conexión a RabbitMQ y declara la cola.
        
        Args:
            host: Hostname del servidor RabbitMQ
            queue_name: Nombre de la cola a usar
        """

        # Crear conexión TCP a RabbitMQ
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host)
        )
        
        # Crear canal
        self.channel = self.connection.channel()
        
        # Guardar nombre de cola
        self.queue_name = queue_name
        
        # Declarar la cola (si ya existe y tiene los mismo parametros no pasa nada)
        self.channel.queue_declare(
            queue=queue_name,
            durable=True 
        )
        
        # 1 solo ack a la vez
        self.channel.basic_qos(prefetch_count=1)

    def start_consuming(self, on_message_callback):
        """
        Comienza a consumir mensajes de la cola.
        
        Args:
            on_message_callback: función
        """
        def internal_callback(ch, method, properties, body):
            # Crear wrappers ack y nack para el usuario
            ack = lambda: ch.basic_ack(delivery_tag=method.delivery_tag)
            nack = lambda: ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            
            on_message_callback(body, ack, nack)
        
        # Registrar callback para consumir mensajes
        self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=internal_callback,
            auto_ack=False  # ACK manual!!
        )
        
        self.channel.start_consuming()
    
    def stop_consuming(self):
        """Detiene el consumo de mensajes."""
        if self.channel and self.channel.is_open:
            self.channel.stop_consuming()
    
    def send(self, message):
        """
        Envía un mensaje a la cola.
        
        Args:
            message: bytes a enviar
        """
        self.channel.basic_publish(
            exchange='',  # usar cola directamente
            routing_key=self.queue_name,
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=2  # persistencia
            )
        )

    def close(self):
        """Cierra la conexión con RabbitMQ."""
        if self.channel and self.channel.is_open:
            self.channel.close()
        if self.connection and self.connection.is_open:
            self.connection.close()


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    """
    Implementación de exchange usando RabbitMQ.
    """
    
    def __init__(self, host, exchange_name, routing_keys):
        """
        Inicializa la conexión a RabbitMQ y declara el exchange.
        
        Args:
            host: Hostname del servidor RabbitMQ
            exchange_name: Nombre del exchange
            routing_keys: Lista de routing keys para bindear
        """
        # Crear conexión y canal
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host)
        )
        self.channel = self.connection.channel()
        
        # Guardar parámetros
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        
        # Declarar exchange 
        self.channel.exchange_declare(
            exchange=exchange_name,
            exchange_type='topic', #permite wildcards en routing keys
            durable=True
        )
        
        # Si hay routing keys, crear cola exclusiva para consumir (cada consumir tiene una sola cola temporal)
        if routing_keys:
            result = self.channel.queue_declare(queue='', exclusive=True)
            self.queue_name = result.method.queue  # Cola con nombre autogenerado
            
            # Bindear la cola al exchange con cada routing key
            for key in routing_keys:
                self.channel.queue_bind(
                    exchange=exchange_name,
                    queue=self.queue_name,
                    routing_key=key
                )
        else:
            self.queue_name = None
        
        self.channel.basic_qos(prefetch_count=1)

    def start_consuming(self, on_message_callback):
        """
        Comienza a consumir mensajes del exchange.
        
        Args:
            on_message_callback: función con firma (message, ack, nack)
        """
        def internal_callback(ch, method, properties, body):
            ack = lambda: ch.basic_ack(delivery_tag=method.delivery_tag)
            nack = lambda: ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            on_message_callback(body, ack, nack)
        
        self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=internal_callback,
            auto_ack=False
        )
        
        self.channel.start_consuming()
    
    def stop_consuming(self):
        """Detiene el consumo de mensajes."""
        if self.channel and self.channel.is_open:
            self.channel.stop_consuming()
    
    def send(self, message):
        """
        Envía un mensaje al exchange.
        
        Args:
            message: bytes a enviar
        """
        # Publicar a cada routing key configurada
        for key in self.routing_keys:
            self.channel.basic_publish(
                exchange=self.exchange_name,
                routing_key=key,
                body=message,
                properties=pika.BasicProperties(delivery_mode=2) # persistencia
            )

    def close(self):
        """Cierra la conexión con RabbitMQ."""
        if self.channel and self.channel.is_open:
            self.channel.close()
        if self.connection and self.connection.is_open:
            self.connection.close()