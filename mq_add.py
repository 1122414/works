import pika
import json
import aw_transfer


class MQ:
    def __init__(self) -> None:
        self.recv_data = []
        self.send_data = []

        self.recv_ip = "172.16.19.108"
        self.recv_port = "5672"
        self.recv_username = "spider"
        self.recv_password = "spider"

        self.send_ip = "172.16.19.108"
        self.send_port = "5672"
        self.send_username = "spider"
        self.send_password = "spider"

        self.recvMQ = self.mqinit(self.recv_ip, self.recv_port, self.recv_username, self.recv_password)
        self.sendMQ = self.mqinit_send(self.send_ip, self.send_port, self.send_username, self.send_password)

        self.recv_keys = ["aw_topic" ,"aw_user","aw_page" ,"aw_goods" , "aw_goods_comment"]

    def mqinit(self,ip, port, username, password):
        credentials = pika.PlainCredentials(username, password)
        connection = pika.BlockingConnection(pika.ConnectionParameters(ip, port, '/anwang4', credentials))
        return connection.channel()
    
    def mqinit_send(self,ip, port, username, password):#TODO 创建新队列
        credentials = pika.PlainCredentials(username, password)
        connection = pika.BlockingConnection(pika.ConnectionParameters(ip, port, '/', credentials))
        return connection.channel()

    def mqRecv(self):
        for recv_key in self.recv_keys:
            self.recvMQ.queue_declare(queue=recv_key, durable=True)
            self.recvMQ.basic_consume(queue=recv_key, on_message_callback=self.recv, auto_ack=False)

        self.recvMQ.start_consuming()

    def recv(self, ch, method, properties, body):
        try:
            data = json.loads(body.decode('utf-8'))
            self.recv_data.append(data)
            self.send()
        except Exception as e:
            print(f"ERROR: recv data: {e} ")

    def send(self):
        mq.mqSend()
        while self.recv_data:
            data = self.recv_data.pop(0)

            if data["table_type"] == "page":
                page, site = aw_transfer.page2(data)
                if page:
                    self.send_data.append({"queue":"page", "data":page})
                if site:
                    self.send_data.append({"queue":"site", "data":site})

            if data["table_type"] == "user":
                user = aw_transfer.user2(data)
                if user:
                    self.send_data.append({"queue":"user", "data":user})

            if data["table_type"] == "goods":
                good = aw_transfer.good2(data)
                if good:
                    self.send_data.append({"queue":"goods", "data":good})
            
            if data["table_type"] == "topic":
                post = aw_transfer.post2(data)
                if post:
                    self.send_data.append({"queue":"post", "data":post})

            if data["table_type"] == "goods_comment":
                pass
                # NOTE comment 数据有问题
                # good = aw_transfer.goodComment2(good,data)
                # if good:
                #     self.send_data.append({"queue":"goods", "data":good})
        print(self.send_data[-1])
    def mqSend(self):
        send_len = len(self.send_data)
        while send_len:
            send_len -= 1

            element = self.send_data.pop(0)
            message = json.dumps(element["data"])
            routing_key = element["queue"]

            try:
                # 检查队列是否存在
                if not self.sendMQ.queue_declare(queue=routing_key, durable=True).method.queue:
                    print(f"Queue '{routing_key}' does not exist. Creating it.")
                    # 创建队列，durable=True 表示队列将在 broker 重启后依然存在
                    self.sendMQ.exchange_declare(exchange="scrapy", exchange_type="direct", durable=True)
                    self.sendMQ.queue_declare(queue=routing_key, durable=True)
                    self.sendMQ.queue_bind(exchange="scrapy",queue=routing_key, routing_key=routing_key)

                # 发布消息到队列
                self.sendMQ.basic_publish(exchange='', routing_key=routing_key, body=message)

            except Exception as e:
                print("mq通道关闭" + str(e))
                self.sendMQ = self.mqinit(self.send_ip, self.send_port, self.send_username, self.send_password)
                # TODO 异常处理需要修改，将发送失败数据添加队列
                self.send_data.append(element)
                break
                # self.sendMQ.basic_publish(exchange='', routing_key=routing_key, body=message)

    def close(self):
        self.recvMQ.close()
        self.sendMQ.close()


if __name__ == "__main__":
    mq = MQ()
    try:
        mq.mqRecv()
        # mq.mqSend()
    except Exception as e:
        mq.close()
        print(f"ERROR: {e}")
