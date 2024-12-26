import pika
import json
import aw_transfer
import logging
from datetime import datetime
from clickhouse_driver import Client
from typing import List, Dict, Any, Optional

# 初始化 ClickHouse 客户端


# 配置日志记录器
log_filename = datetime.now().strftime("%Y%m%d") + ".log"
logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    # encoding="utf-8",  # 指定字符集为 UTF-8
)

class ClickHouse:
    def __init__(self, host: str = "172.16.19.40", port: int = 9000, user: str = "default", password: str = "", database: str = "ty"):
        """
        初始化 ClickHouse 连接
        """
        # 配置 ClickHouse 连接信息
        self.host = host # 替换为 ClickHouse 服务器地址
        self.port = port # 默认端口是 9000
        self.user = user # 默认用户
        # password = 'inspur@123'                  # 默认没有密码，若有密码请填写
        self.password = password              # 默认没有密码，若有密码请填写
        self.database = database

    def connection(self):
        self.client = Client(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database
        )
    
    def pre_query(self, id, url, mode):
        query = f"select * from mid_deepweb_goods where '{mode}'='{id}' and url='{url}'"
        result = client.execute_query(query=query)
        return result

    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        执行查询并返回结果（以字典列表形式返回）
        :param query: 查询语句
        :param params: 可选的参数，用于查询中的占位符
        :return: 查询结果，列表形式，每行数据为字典
        """
        try:
            # 执行查询
            rows = self.client.execute(query, params)
            # 获取列名
            columns = [desc[0] for desc in self.client.execute("DESCRIBE TABLE mid_deepweb_goods")]

            # 将结果转为键值对的字典列表
            return [dict(zip(columns, row)) for row in rows]

        except Exception as e:
            # print(f"Error executing query: {e}")
            logging.error(f"ERROR: clickhouse 查询出错: {e} ")
            self.connection()
            return 0




class MQ:
    def __init__(self) -> None:
        self.recv_data = []
        self.send_data = []
        self.recv_count = 0  # 初始化计数器
        self.send_count = {}  # 初始化计数器
        self.send_count["page_num"] = 0
        self.send_count["site_num"] = 0
        self.send_count["post_num"] = 0
        self.send_count["user_num"] = 0
        self.send_count["goods_num"] = 0

        self.recv_ip = "172.16.19.108"
        self.recv_port = "5672"
        self.recv_username = "spider"
        self.recv_password = "spider"
        self.send_ip = "172.16.19.108"
        self.send_port = "5672"
        self.send_username = "spider"
        self.send_password = "spider"

        self.recv_keys = ["aw_topic", "aw_page", "aw_user", "aw_goods", "aw_goods_comment"]
        # self.recv_keys = ["aw_topic", "aw_page", "aw_user", "aw_goods"]

        self.clickhouse = ClickHouse()

        self.connection()

    def mqinit(self, ip, port, username, password, vhost):
        credentials = pika.PlainCredentials(username, password)
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(ip, port, vhost, credentials)
        )
        return connection.channel()

    def connection(self):
        self.recvMQ = self.mqinit(
            self.recv_ip, self.recv_port, self.recv_username, self.recv_password,
            "/anwang4",
        )
        self.sendMQ = self.mqinit(
            self.send_ip, self.send_port, self.send_username,
            self.send_password, "/"
        )
        self.clickhouse.connection()

    def mqRecv(self):
        while True:
            try:
                self.recvMQ.queue_declare(queue="dark_service", durable=True)
                self.recvMQ.basic_consume(
                    queue="dark_service", on_message_callback=self.recv, auto_ack=False
                )
                for recv_key in self.recv_keys:
                    self.recvMQ.queue_declare(queue=recv_key, durable=True)
                    self.recvMQ.basic_consume(
                        queue=recv_key, on_message_callback=self.recv, auto_ack=False
                    )

                self.recvMQ.start_consuming()
            except:
                self.close()
                self.connection()

    def recvsite(self, ch, method, properties, body):
        try:
            data = json.loads(body.decode("utf-8"))

            site = aw_transfer.site3(data)
            if site:
                self.send_data.append({"queue": "site", "data": site})

            logging.info(
                f"******接收数据 site，累计接收次数: {self.recv_count}******"
            )
            # ch.basic_ack(delivery_tag=method.delivery_tag) #TODO 手动确认
            mq.mqSend()

        except Exception as e:
            logging.error(f"ERROR: recv data: {e} ")

    def recv(self, ch, method, properties, body):
        try:
            data = json.loads(body.decode("utf-8"))

            if data["table_type"] == "page":
                page, site = aw_transfer.page2(data)
                if page:
                    self.send_data.append({"queue": "page", "data": page})
            #  if site:
            #      self.send_data.append({"queue":"site", "data":site})

            if data["table_type"] == "user":
                user = aw_transfer.user2(data)
                if user:
                    self.send_data.append({"queue": "user", "data": user})

            if data["table_type"] == "goods":
                good = aw_transfer.good2(data)
                if good:
                    self.send_data.append({"queue": "goods", "data": good})

            if data["table_type"] == "topic":
                post = aw_transfer.post2(data)
                if post:
                    self.send_data.append({"queue": "post", "data": post})

            if data["table_type"] == "goods_comment":
                if data['comment_type'] == 'goods':
                    good = self.clickhouse.pre_query(data["commented_id"], data["url"], "id")
                elif data['comment_type'] == 'user':
                    good = self.clickhouse.pre_query(data["user_name"], data["url"], "user_name")

                if not good:
                    # 查询出错 或 没有查询到 推送到备用队列
                    self.push("aw_"+data["table_type"], json.dumps(data))
                    return None

                # 错误处理
                if(len(good)==0):
                    logging.error(f"ERROR: 未查询到商品数据")
                    return
                
                if(len(good)>1):
                    logging.error(f"ERROR: 查询到多个商品数据")
                    return
                # 合并后的数据加入上传mq的队列
                good_cmb = aw_transfer.good_cmb2(good[0], data)
                self.send_data.append({"queue": "goods", "data": good_cmb})

            self.recv_count += 1  # 增加计数
            logging.info(
                f"******接收数据{data['table_type']}，累计接收次数: {self.recv_count}******"
            )
            # ch.basic_ack(delivery_tag=method.delivery_tag) #TODO 手动确认
            mq.mqSend()

        except Exception as e:
            logging.error(f"ERROR: recv data: {e} ")
            self.push("aw_"+data["table_type"], json.dumps(data))

    def mqSend(self):
        send_len = len(self.send_data)
        while send_len:
            send_len -= 1

            element = self.send_data.pop(0)
            message = json.dumps(element["data"])
            # routing_key = element["queue"]
            routing_key = element["queue"]

            if self.push(routing_key, message):
                # self.send_count += 1  # 初始化计数器
                self.send_count[routing_key + "_num"] += 1  # 增加计数
                logging.info(
                    f"------发送数据{routing_key}，累计发送{routing_key}次数: {self.send_count[routing_key+'_num']}------\n"
                )
            else:
                self.send_data.append(element)

    def push(self, routing_key, message):
        try:
            # 检查队列是否存在
            if not self.sendMQ.queue_declare(
                queue=routing_key, durable=True
            ).method.queue:
                logging.info(f"Queue '{routing_key}' does not exist. Creating it.")
                # 创建队列，durable=True 表示队列将在 broker 重启后依然存在
                self.sendMQ.exchange_declare(
                    exchange="scrapy", exchange_type="direct", durable=True
                )
                self.sendMQ.queue_declare(queue=routing_key, durable=True)
                self.sendMQ.queue_bind(
                    exchange="scrapy", queue=routing_key, routing_key=routing_key
                )

            # 发布消息到队列
            self.sendMQ.basic_publish(
                exchange="", routing_key=routing_key, body=message
            )

            return True

        except Exception as e:
            logging.error("mq通道关闭" + str(e))
            self.sendMQ = self.mqinit(
                self.send_ip,
                self.send_port,
                self.send_username,
                self.send_password,
                "/",
            )

            return False

    def close(self):
        try:
            self.recvMQ.close()
            self.sendMQ.close()
        except:
            logging.error("Channel is already closed.")

if __name__ == "__main__":
    mq = MQ()
    try:
        mq.mqRecv()
        mq.close()
        mq.connection()
    except Exception as e:
        logging.error(f"ERROR: {e}")
        mq.close()
        mq.connection()
        mq.mqRecv()
