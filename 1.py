import pika
import pprint
import json

ip = "43.154.141.197"
port = 5672
username = "spider"
password = "spider"

credentials = pika.PlainCredentials(username, password)
connection = pika.BlockingConnection(pika.ConnectionParameters(ip, port, '/', credentials))
channel = connection.channel()

method_frame, header_frame, body = channel.basic_get(queue="site", auto_ack=False)
method_frame, header_frame, body1 = channel.basic_get(queue="user", auto_ack=False)
method_frame, header_frame, body2 = channel.basic_get(queue="page", auto_ack=False)


message_dict = json.loads(body.decode("utf-8"))

print("site")
for key, value in message_dict.items():
    print(f"{key}:{value} = {type(value)}")

message_dict1 = json.loads(body1.decode("utf-8"))

print("user")
for key, value in message_dict1.items():
    print(f"{key}:{value} = {type(value)}")

message_dict2 = json.loads(body2.decode("utf-8"))

print("page")
for key, value in message_dict2.items():
    print(f"{key}:{value} = {type(value)}")

print(type(body2))
print(type(message_dict2))
