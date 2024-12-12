import pika
import json
from aw_transfer import openjson, page2, user2, post2, good2, site2

# ip = "172.16.19.108"
ip = "43.154.182.55"
port = 5672
username = "spider"
password = "spider"

credentials = pika.PlainCredentials(username, password)
connection = pika.BlockingConnection(pika.ConnectionParameters(ip, port, '/', credentials))
channel = connection.channel()

pages = openjson("aw_page.json")
page, site = page2(pages[0])
# site = site2(pages[0])
print(f"page:\n{page}")
print(f"site:\n{site}")
users = openjson("aw_user.json")
user = user2(users[0])
print(f"user:\n{user}")
topic = openjson("aw_topic.json")
post = post2(topic[0])
print(f"post:\n{post}")
goods = openjson("aw_goods.json")
good = good2(goods[0])
print(f"good:\n{good}")

jpost = json.dumps(post)
jgood = json.dumps(good)
jpage = json.dumps(page)
jsite = json.dumps(site)
juser = json.dumps(user)

with open("test_post.json", 'w', encoding='utf-8') as f:
    f.write(jpost)
with open("test_good.json", 'w', encoding='utf-8') as f:
    f.write(jgood)
with open("test_page.json", 'w', encoding='utf-8') as f:
    f.write(jpage)
with open("test_site.json", 'w', encoding='utf-8') as f:
    f.write(jsite)
with open("test_user.json", 'w', encoding='utf-8') as f:
    f.write(juser)

egoods = openjson("good.json")
epages = openjson("page.json")
eposts = openjson("post.json")
esites = openjson("site.json")
eusers = openjson("user.json")

for key in egoods[0]:
    if type(egoods[0][key]) != type(good[key]):
        print(f"good: {key} : {type(egoods[0][key])}")
print("good finish")

for key in epages[0]:
    if type(epages[0][key]) != type(page[key]):
        print(f"page: {key} : {type(epages[0][key])}")
print("page finish")

for key in eposts[0]:
    if type(eposts[0][key]) != type(post[key]):
        print(f"post: {key} : {type(eposts[0][key])}")
print("post finish")

for key in esites[0]:
    if type(esites[0][key]) != type(site[key]):
        print(f"site: {key} : {type(esites[0][key])}")
print("site finish")

for key in eusers[0]:
    if type(eusers[0][key]) != type(user[key]):
        print(f"user: {key} : {type(eusers[0][key])}")
print("user finish")

# 发布消息到队列
channel.basic_publish(exchange='', routing_key="post", body=jpost)
channel.basic_publish(exchange='', routing_key="goods", body=jgood)
channel.basic_publish(exchange='', routing_key="page", body=jpage)
channel.basic_publish(exchange='', routing_key="site", body=jsite)
channel.basic_publish(exchange='', routing_key="user", body=juser)
