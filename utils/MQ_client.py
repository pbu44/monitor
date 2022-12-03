# import os
from azure.servicebus import ServiceBusClient, ServiceBusMessage
import json
CONNECTION_STR = "Endpoint=sb://newsbytes.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=B/NPwLAY1n7HO34/9+if0bJWcCmQ1BJ3SSnVVnRxqEg="


class CloudAMQPClient:
    QUEUE_NAME = ""
    def __init__(self,queue_name):
        print(CONNECTION_STR)
        self.servicebus_client = ServiceBusClient.from_connection_string(conn_str=CONNECTION_STR, logging_enable=True)
        self.QUEUE_NAME = queue_name
    # send a message
    def sendMessage(self, message):
        # message is json object, when send message to queue,
        # we need to convert it to string
        string_message = json.dumps(message)
        msg = ServiceBusMessage(string_message)
        sender = self.servicebus_client.get_queue_sender(queue_name=self.QUEUE_NAME)
        sender.send_messages(msg)
       
    def getMessage(self):
        
        # if error, method_frame null
        receiver = self.servicebus_client.get_queue_receiver(queue_name=self.QUEUE_NAME, max_wait_time=5)
        if receiver:
            msg = None
            for msg in receiver:
                print("Received: " + str(msg))
                receiver.complete_message(msg)
            return msg
        else:
            print ("No message returned")
            return None