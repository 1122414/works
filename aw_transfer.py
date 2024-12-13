import json
from bs4 import BeautifulSoup
import re
import hashlib


def calculate_sha1(data):
    # 创建一个新的sha1 hash对象
    hash_object = hashlib.sha1()
    # 提供需要散列的数据
    hash_object.update(data.encode())
    # 获取十六进制格式的散列值
    return hash_object.hexdigest()


def openjson(file):
    with open(file, encoding='utf-8') as jsonfile:
        data = json.load(jsonfile)
    return data


def site2(page):
    if page["table_type"] != "page":
        return None

    site = {
            "domain": "",
            "net_type": "",
            "url": "",
            "title": "",
            "description": "[]",
            "lang": "",
            "snapshot": "{}",
            "name": "",
            "path": "",
            "image_hash": "",
            "last_status": "",
            "first_publish_time": "",
            "last_publish_time": "",
            "service_type": "",
            "is_recent_online": "",
            "scale": "{}",
            "active_level": "[]",
            "label": "[]",
            "site_hazard": "[]",
            "goods_label": "[]",
            "goods_count": -1,
            "pay_methods": "[]",
            "goods_user_count": -1,
            "platform": "[]",
            "content_encode": "",
            "site_name": "",
            "index_url": "",
            "user_info": "{}",
    }

    site["domain"] = page["url"][:page["url"].index("//")+2]+page["domain"]
    site["net_type"] = page["net_type"]
    site["url"] = page["url"]
    site["lang"] = "en_us" if page["language"] == "en" else page["language"]
    site["first_publish_time"] = page["crawl_time"]
    site["last_publish_time"] = page["crawl_time"]
    site["is_recent_online"] = "true"
    site["index_url"] = page["url"]

    try:
        site["title"] = page["title"]
        site["platform"] = page["title"]
        site["site_name"] = page["title"]
    except Exception as e:
        print(e)
        site["title"] = ""
        site["platform"] = ""
        site["site_name"] = ""

    try:
        site["content_encode"] = page["meta"]["charset"]
    except Exception as e:
        print(e)
        site["content_encode"] = ""

    return site


def page2(page):
    if page["table_type"] != "page":
        return None, None

    apage = {
            "platform": "",
            "crawl_time": "",
            "domain": "",
            "content_encode": "",
            "lang": "",
            "meta": "",
            "net_type": "{}",
            "page_source": "[]",
            "title": "",
            "url": "",
            "publish_time": "",
            "subject": "[]",
            "content": "",
            "label": "{}",
            "snapshot_name": "",
            "snapshot_oss_path": "",
            "snapshot_hash": "",
            "warn_topics": "[]",
            "url_and_address": "[]",
            "extract_entity": "[]",
            "images_obs": "{}",
            "field_name": "[]",
    }

    apage["crawl_time"] = page["crawl_time"]
    apage["domain"] = page["url"][:page["url"].index("//")+2]+page["domain"]
    apage["lang"] = "en_us" if page["language"] == "en" else page["language"]
    apage["net_type"] = page["net_type"]
    apage["page_source"] = page["page_source"]
    apage["url"] = page["url"]
    apage["publish_time"] = page["crawl_time"]

    try:
        soup = BeautifulSoup(page["page_source"], 'html.parser')
        text = soup.get_text()
        text = re.sub(r'\n+', '\n', text)
        apage["content"] = text
    except Exception as e:
        print(e)
        apage["content"] = ""

    try:
        apage["subject"] = page["h1"]
    except Exception as e:
        print(e)
        apage["subject"] = ""
    try:
        apage["title"] = page["title"]
    except Exception as e:
        print(e)
        apage["title"] = ""
    try:
        apage["meta"] = str(page["meta"])
    except Exception as e:
        print(e)
        apage["meta"] = ""
    try:
        apage["content_encode"] = page["meta"]["charset"]
    except Exception as e:
        print(e)
        apage["content_encode"] = ""

    site = {}

    if apage["domain"] == apage["url"] or apage["domain"] == apage["url"][:-1]:
        site = site2(page)

    return apage, site

def user2(user):
    if user["table_type"] != "user":
        return None

    auser = {
            "platform": "",
            "uuid": "",
            "domain": "",
            "net_type": "",
            "user_name": "",
            "user_description": "[]",
            "user_id": "",
            "url": "",
            "user_nickname": "[]",
            "identity_tags": "[]",
            "register_time": "",
            "last_active_time": "",
            "goods_orders": -1,
            "level": "",
            "member_degree": "",
            "ratings": "[]",
            "user_img": "{}",
            "topic_nums": 1,
            "area": "[]",
            "user_verification": "[]",
            "user_order_count": -1,
            "user_viewed_count": -1,
            "user_feedback_count": -1, 
            "user_followed_count": -1,
            "emails": "[]",
            "bitcoin_addresses": "[]",
            "eth_addresses": "[]",
            "crawl_time": "",
            "user_hazard_level": "[]",
            "post_counts": 0,
            "user_related_url_and_address": "",
            "user_related_images": "",
            "user_related_files": "",
            "user_recent_day": -1,
            "user_related_crawl_tags": "",
            "lang": "",
    }

    auser["domain"] = user["url"][:user["url"].index("//")+2]+user["domain"]
    auser["uuid"] = calculate_sha1(user["domain"]+user["user_id"])
    # auser["uuid"] = user["uuid"]
    auser["net_type"] = user["net_type"]
    auser["user_name"] = user["user_name"]
    auser["user_id"] = user["user_id"]
    auser["url"] = user["url"]
    auser["crawl_time"] = user["crawl_time"]

    return auser

def post2(topic):
    if topic["table_type"] != "topic":
        return None
    post = {
                "platform": "", #
                "uuid": "", #
                "user_id": "", #
                "user_name": "", #
                "publish_time": "", #
                "content": "", #
                "topic_id": "", #
                "url": "", #
                "title": "", #
                "crawl_time": "", #
                "net_type": "", #
                "topic_type": "", #
                "domain": "", #
                "crawl_tags": "[]", #
                "comment_id": "", #
                "commented_user_id": "[]", #
                "commented_id": "[]", #
                "commented_count": 0, #
                "clicks_times": -1, #
                "thumbs_up": -1, #
                "thumbs_down": -1,  #
                "images": "{}", #
                "attachments": "{}", #
                "emails": "[]", #
                "bitcoin_addresses": "[]", #
                "eth_addresses": "[]", #
                "lang": "", #
                "label": "{}", #
                "extract_entity": "[]", #
                "threaten_level": "[]", #
                "post_id": "", #
                "images_obs": "{}", #
                "attachments_obs": "{}", #
                "update_time": "", #
                "commented_user_names": "[]", #
                "url_and_address": "[]", #
    }

    post["domain"] = topic["url"][:topic["url"].index("//")+2]+topic["domain"]
    post["uuid"] = topic["uuid"]
    post["post_id"] = topic["uuid"]
    # post["user_id"] = topic["user_id"]
    post["user_id"] = calculate_sha1(topic["domain"]+topic["user_id"])
    post["user_name"] = topic["user_name"]
    post["publish_time"] = topic["publish_time"]
    post["content"] = topic["content"]
    post["topic_id"] = topic["topic_id"]
    post["topic_type"] = topic["topic_type"]
    post["url"] = topic["url"]
    post["crawl_time"] = topic["crawl_time"]
    post["net_type"] = topic["net_type"]

    try:
        post["emails"] = str(topic["emails"])
    except Exception as e:
        print(e)
        post["emails"] = ""

    try:
        post["title"] = topic["title"]
    except Exception as e:
        print(e)
        post["title"] = ""

    try:
        post["comment_id"] = topic["comment_id"]
        if topic["topic_type"] == "post":
            post["comment_id"] = ""
    except Exception as e:
        print(e)
        post["comment_id"] = ""
    try:
        # post["comment_user_id"] = topic["commented_user_id"]
        post["commented_user_id"] = calculate_sha1(topic["domain"]+topic["commented_user_id"])
    except Exception as e:
        print(e)
        post["comment_user_id"] = ""
    try:
        post["bitcoin_addresses"] = str(topic["bitcoin_addresses"])
    except Exception as e:
        print(e)
        post["bitcoin_addresses"] = ""
    try:
        post["eth_addresses"] = str(topic["eth_addresses"])
    except Exception as e:
        print(e)
        post["eth_addresses"] = ""

    return post

def good2(goods):
    if goods["table_type"] != "goods":
        return None
    good = {
                "platform": "",
                "uuid": "",
                "domain": "",
                "goods_id": "",
                "goods_name": "",
                "goods_info": "",
                "images": "",
                "attachments": "",
                "bitcoin_addresses": "",
                "contacts": "",
                "crawl_category": "",
                "crawl_category_1": "",
                "crawl_time": "",
                "goods_area": "",
                "goods_browse_count": -1,
                "goods_buyer": "",
                "goods_feedback_count": -1,
                "comment_user_id": "",
                "comment_id": "",
                "comment_time": "",
                "comment_content": "",
                "goods_ship_to": "",
                "goods_tag": "",
                "goods_update_time": "",
                "net_type": "",
                "price": "",
                "publish_time": "",
                "sku_quantify": "",
                "sold_count": -1,
                "url": "",
                "user_id": "",
                "user_name": "",
                "lang": "",
                "url_and_address": "",
                "keywords_by_nlp": "",
                "threaten_level": "",
                "images_obs": "",
                "attachments_obs": "",
    }

    good["uuid"] = goods["uuid"]
    good["domain"] = goods["domain"]
    good["goods_id"] = goods["goods_id"]
    good["goods_name"] = goods["goods_name"]
    good["goods_info"] = goods["goods_info"]
    good["images"] = str(goods["goods_img_url"])
    good["crawl_category"] = goods["crawl_category"]
    good["crawl_time"] = goods["crawl_time"]
    good["net_type"] = goods["net_type"]
    good["publish_time"] = goods["publish_time"]
    good["url"] = goods["url"]
    # good["user_id"] = goods["user_id"]
    good["user_id"] = calculate_sha1(goods["domain"]+goods["user_id"])
    good["user_name"] = goods["user_name"]

    try:
        good["price"] = str(goods["price"][0])
    except Exception as e:
        print(e)
        good["price"] = str(goods["price"])
    try:
        good["bitcoin_addresses"] = str(goods["bitcoin_addresses"])
    except Exception as e:
        print(e)
        good["bitcoin_addresses"] = ""
    try:
        good["goods_tag"] = str(goods["goods_tag"])
    except Exception as e:
        print(e)
        good["goods_tag"] = ""
    try:
        good["sku_quantify"] = goods["sku"]
    except Exception as e:
        print(e)
        good["sku_quantify"] = ""

    return good

def goodComment2(good,comment):
    if comment["table_type"] != "goods_comment":
        return None

    # good["comment_user_id"] = comment["user_id"]
    good["comment_user_id"] = calculate_sha1(comment["domain"]+comment["user_id"])
    good["comment_id"] = comment["comment_id"]
    good["comment_time"] = comment["publish_time"]
    good["comment_content"] = comment["content"]

    return good


if __name__ == "__main__":
    page = openjson("aw_page.json")
    apage, site = page2(page[0])
    # print(apage)
    user = openjson("aw_user.json")
    auser = user2(user[0])
    print(auser)
    topic = openjson("aw_topic.json")
    post = post2(topic[0])
    print(post)
    goods = openjson("aw_goods.json")
    good = good2(goods[0])
    print(good)
