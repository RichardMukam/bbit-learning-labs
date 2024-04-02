import pika
import os
import sys
from consumer_interface import mqConsumerInterface

class mqConsumer(mqConsumerInterface):
  def __init__(self, binding_key, exchange_name, queue_name):
    self.binding_key = binding_key
    self.exchange_name = exchange_name
    self.queue_name = queue_name
    self.setupRMQConnection()

  def setupRMQConnection(self) -> None:
        # Set-up Connection to RabbitMQ service
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection = pika.BlockingConnection(parameters=con_params)
        # Establish Channel
        self.channel = self.connection.channel()
        # Create Queue if not already present
        self.channel.queue_declare(queue=self.queue_name)
        # Create the exchange if not already present
        self.channel.exchange_declare(
            exchange=self.exchange_name
        )        
        self.channel.queue_bind(
            exchange=self.exchange_name,
            queue=self.queue_name,
            routing_key=self.binding_key,
        )        
        # Set-up Callback function for receiving messages
        self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=self.on_message_callback,
            auto_ack=False,
        )

  def on_message_callback(self, channel, method_frame, header_frame, body) -> None:
       # Acknowledge message
        channel.basic_ack(method_frame.delivery_tag, False)
        #Print message (The message is contained in the body parameter variable)
        print(body)

  def startConsuming(self) -> None:
        print(" [*] Waiting for messages. To exit press CTRL+C")
        self.channel.start_consuming()
  
  def __del__(self) -> None:
        # Print "Closing RMQ connection on destruction"
        print("Closing RMQ connection on destruction")
        # Close Channel
        self.channel.close()
        # Close Connection
        self.connection.close()