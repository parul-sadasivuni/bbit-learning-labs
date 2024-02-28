from consumer_interface import mqConsumerInterface

import pika
import os



class mqConsumer(mqConsumerInterface):
    def __init__(
        self, binding_key: str, exchange_name: str, queue_name: str
    ) -> None:
        # Save parameters to class variables
        self.exchangeName = exchange_name;
        self.queue = queue_name;
        self.key = binding_key; 
        # Call setupRMQConnection
        self.setupRMQConnection()
        

    def setupRMQConnection(self) -> None:
        # Set-up Connection to RabbitMQ service
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        connection = pika.BlockingConnection(parameters=con_params)
        # Establish Channel
        self.channel = connection.channel()
        # Create Queue if not already present
        self.channel.queue_declare(self.queue)
        # Create the exchange if not already present
        self.channel.exchange_declare(self.exchangeName)
        # Bind Binding Key to Queue on the exchange
        self.channel.queue_bind (
            queue = self.queue,
            routing_key = self.key,
            exchange = self.exchangeName,
        )
        # Set-up Callback function for receiving messages
        self.channel.basic_consume(
            self.queue, self.on_message_callback, auto_ack=False
        )
        pass

    def on_message_callback(
        self, channel, method_frame, body
    ) -> None:
        # Acknowledge message
        channel.basic_ack(method_frame.delivery_tag, False)
        #Print message
        print(body)
        pass

    def startConsuming(self) -> None:
        # Print " [*] Waiting for messages. To exit press CTRL+C"
        print(" [*] Waiting for messages. To exit press CTRL+C")
        # Start consuming messages
        self.channel.start_consuming()
        pass