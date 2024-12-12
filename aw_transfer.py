import json
from bs4 import BeautifulSoup
import re

def openjson(file):
    with open(file, encoding='utf-8') as jsonfile:
        data = json.load(jsonfile)
    return data

def site2(page):
    if page["table_type"] != "page":
        return None
    site = {
            "domain" : "",
            "net_type" : "",
            "url" : "",
            "title" : "",
            "description" : "[]",
            "lang" : "",
            "snapshot" : "{}",
            "name" : "",
            "path" : "",
            "image_hash" : "",
            "last_status" : "",
            "first_publish_time" : "",
            "last_publish_time" : "",
            "service_type" : "",
            "is_recent_online" : "",
            "scale" : "{}",
            "active_level" : "[]",
            "label" : "[]",
            "site_hazard" : "[]",
            "goods_label" : "[]",
            "goods_count" : -1,
            "pay_methods" : "[]",
            "goods_user_count" : -1,
            "platform" : "[]",
            "content_encode" : "",
            "site_name" : "",
            "index_url" : "",
            "user_info" : "{}",
    }

    site["domain"] = page["url"][:page["url"].index(page["domain"])]+page["domain"]
    site["net_type"] = page["net_type"]
    site["url"] = page["url"]
    site["title"] = page["title"]
    site["lang"] = "en_us" if page["language"] == "en" else page["language"]
    site["first_publish_time"] = page["crawl_time"]
    site["last_publish_time"] = page["crawl_time"]
    # site["last_publish_time"] = page["crawl_time"]
    site["is_recent_online"] = "true"
    site["platform"] = page["title"]
    try:
        site["content_encode"] = page["meta"]["charset"]
    except:
        site["content_encode"] = ""
    site["site_name"] = page["title"]
    site["index_url"] = page["url"]

    print(site)
    return site

def page2(page):
    if page["table_type"] != "page":
        return None, None

    apage = {
            "platform" : "",
            "crawl_time" : "",
            "domain" : "",
            "content_encode" : "",
            "lang" : "",
            "meta" : "",
            "net_type" : "{}",
            "page_source" : "[]",
            "title" : "",
            "url" : "",
            "publish_time" : "",
            "subject" : "[]",
            "content" : "",
            "label" : "{}",
            "snapshot_name" : "",
            "snapshot_oss_path" : "",
            "snapshot_hash" : "",
            "warn_topics" : "[]",
            "url_and_address" : "[]",
            "extract_entity" : "[]",
            "images_obs" : "{}",
            "field_name" : "[]",
    }

    apage["crawl_time"] = page["crawl_time"]
    apage["domain"] = page["url"][:page["url"].index(page["domain"])]+page["domain"]
    try:
        apage["content_encode"] = page["meta"]["charset"]
    except:
        apage["content_encode"] = ""
    apage["lang"] = "en_us" if page["language"] == "en" else page["language"]
    try:
        apage["meta"] = page["meta"]
    except:
        apage["meta"] = ""
    apage["net_type"] = page["net_type"]
    apage["page_source"] = page["page_source"]
    apage["title"] = page["title"]
    apage["url"] = page["url"]
    apage["publish_time"] = page["crawl_time"]
    try:
        apage["subject"] = page["h1"]
    except:
        apage["subject"] = ""
    soup = BeautifulSoup(page["page_source"], 'html.parser')
    text = soup.get_text()
    text = re.sub(r'\n+', '\n', text)
    apage["content"] = text

    site = {}

    if page["domain"] == page["url"]:
        site = site2(page)

    return apage, site

def user2(user):
    if user["table_type"] != "user":
        return None

    auser = {
            "platform" : "",
            "uuid" : "",
            "domain" : "",
            "net_type" : "",
            "user_name" : "",
            "user_description" : "[]",
            "user_id" : "",
            "url" : "",
            "user_nickname" : "[]",
            "identity_tags" : "[]",
            "register_time" : "",
            "last_active_time" : "",
            "goods_orders" : "",
            "level" : "",
            "member_degree" : "",
            "ratings" : "[]",
            "user_img" : "{}",
            "topic_nums" : 1,
            "area" : "[]",
            "user_verification" : "[]",
            "user_order_count" : -1,
            "user_viewed_count" : -1,
            "user_feedback_count" : -1, 
            "user_followed_count" : -1,
            "emails" : "[]",
            "bitcoin_addresses" : "[]",
            "eth_addresses" : "[]",
            "crawl_time" : "",
            "user_hazard_level" : "[]",
            "post_counts" : 0,
            "user_related_url_and_address" : "",
            "user_related_images" : "",
            "user_related_files" : "",
            "user_recent_day" : "",
            "user_related_crawl_tags" : "",
            "lang" : "",
    }

    auser["uuid"] = user["uuid"]
    auser["domain"] = user["url"][:user["url"].index(user["domain"])]+user["domain"]
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
                "platform" : "", #
                "uuid" : "", #
                "user_id" : "", #
                "user_name" : "", #
                "publish_time" : "", #
                "content" : "", #
                "topic_id" : "", #
                "url" : "", #
                "title" : "", #
                "crawl_time" : "", #
                "net_type" : "", #
                "topic_type" : "", #
                "domain" : "", #
                "crawl_tags" : "[]", #
                "comment_id" : "", #
                "commented_user_id" : "[]", #
                "commented_id" : "[]", #
                "commented_count" : 0, #
                "clicks_times" : -1, #
                "thumbs_up" : -1, #
                "thumbs_down" : -1,  #
                "images" : "{}", #
                "attachments" : "{}", #
                "emails" : "[]", #
                "bitcoin_addresses" : "[]", #
                "eth_addresses" : "[]", #
                "lang" : "", #
                "label" : "{}", #
                "extract_entity" : "[]", #
                "threaten_level" : "[]", #
                "post_id" : "", #
                "images_obs" : "{}", #
                "attachments_obs" : "{}", #
                "update_time" : "", #
                "commented_user_names" : "[]", #
                "url_and_address" : "[]", #
    }

    post["uuid"] = topic["uuid"]
    post["post_id"] = topic["uuid"]
    post["user_id"] = topic["user_id"]
    post["user_name"] = topic["user_name"]
    post["publish_time"] = topic["publish_time"]
    post["content"] = topic["content"]
    post["topic_id"] = topic["topic_id"]
    post["topic_type"] = topic["topic_type"]
    post["url"] = topic["url"]
    post["title"] = topic["title"]
    post["crawl_time"] = topic["crawl_time"]
    post["net_type"] = topic["net_type"]
    post["domain"] = topic["url"][:topic["url"].index(topic["domain"])]+topic["domain"]
    post["emails"] = str(topic["emails"])


    try:
        post["comment_id"] = topic["comment_id"]
    except:
        post["comment_id"] = ""
    try:
        post["comment_user_id"] = topic["commented_user_id"]
    except:
        post["comment_user_id"] = ""
    try:
        post["bitcoin_addresses"] = str(topic["bitcoin_addresses"])
    except:
        post["bitcoin_addresses"] = ""
    try:
        post["eth_addresses"] = str(topic["eth_addresses"])
    except:
        post["eth_addresses"] = ""

    return post

def good2(goods):
    if goods["table_type"] != "goods":
        return None

    good = {
                "platform" : "",
                "uuid" : "",
                "domain" : "",
                "goods_id" : "",
                "goods_name" : "",
                "goods_info" : "",
                "images" : "",
                "attachments" : "",
                "bitcoin_addresses" : "",
                "contacts" : "",
                "crawl_category" : "",
                "crawl_category_1" : "",
                "crawl_time" : "",
                "goods_area" : "",
                "goods_browse_count" : "",
                "goods_buyer" : "",
                "goods_feedback_count" : "",
                "comment_user_id" : "",
                "comment_id" : "",
                "comment_time" : "",
                "comment_content" : "",
                "goods_ship_to" : "",
                "goods_tag" : "",
                "goods_update_time" : "",
                "net_type" : "",
                "price" : "",
                "publish_time" : "",
                "sku_quantify" : "",
                "sold_count" : "",
                "url" : "",
                "user_id" : "",
                "user_name" : "",
                "lang" : "",
                "url_and_address" : "",
                "keywords_by_nlp" : "",
                "threaten_level" : "",
                "images_obs" : "",
                "attachments_obs" : "",
    }

    good["uuid"] = goods["uuid"]
    good["domain"] = goods["domain"]
    good["goods_id"] = goods["goods_id"]
    good["goods_name"] = goods["goods_name"]
    good["goods_info"] = goods["goods_info"]
    good["images"] = goods["goods_img_url"]
    try:
        good["bitcoin_addresses"] = goods["bitcoin_addresses"]
    except:
        good["bitcoin_addresses"] = ""
    good["crawl_category"] = goods["crawl_category"]
    good["crawl_time"] = goods["crawl_time"]
    try:
        good["goods_tag"] = goods["goods_tag"]
    except:
        good["goods_tag"] = ""
    good["net_type"] = goods["net_type"]
    good["price"] = goods["price"][0]
    good["publish_time"] = goods["publish_time"]
    try:
        good["sku_quantify"] = goods["sku"]
    except:
        good["sku_quantify"] = ""
    good["url"] = goods["url"]
    good["user_id"] = goods["user_id"]
    good["user_name"] = goods["user_name"]

    return good

def goodComment2(good,comment):
    if comment["table_type"] != "goods_comment":
        return None

    good["comment_user_id"] = comment["user_id"]
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
    # print(auser)
    topic = openjson("aw_topic.json")
    post = post2(topic[0])
    # print(post)
    goods = openjson("aw_goods.json")
    good = good2(goods[0])
    # print(good)
