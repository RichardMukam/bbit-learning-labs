import pika
import os
import sys

class mqProducer():
    def __init__(self, routing_key: str, exchange_name: str) -> None:
        self.routing_key = routing_key
        self.exchange = exchange_name
        self.setupRMQConnection()
          
    
    def setupRMQConnection(self) -> None:
        #setup connection
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection = pika.BlockingConnection(parameters=con_params)
 
        #establish chhannel
        self.channel = self.connection.channel()
        #creates the exchange
        exchange = self.channel.exchange_declare(
            exchange=self.exchange
        )     
     
    def publishOrder(self, message: str) -> None:

        # Create Appropiate Topic String


        # Send serialized message or String

        
        # Print Confirmation


        #publish
        self.channel.basic_publish(
            exchange=self.exchange,
            routing_key=self.routing_key,
            body=message
        )
        
        # Close channel and connection
        self.channel.close()
        self.connection.close()


