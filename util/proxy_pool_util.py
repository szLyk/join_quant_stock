import requests
from util.read_resource_file import PROXY_POOL_IP, PROXY_POOL_PORT
import socket


def get_proxy():
    return requests.get(f"http://{PROXY_POOL_IP}:{PROXY_POOL_PORT}/get?type=https").json()


def delete_proxy(proxy):
    requests.get(f"http://{PROXY_POOL_IP}:{PROXY_POOL_PORT}" + "/delete/?proxy={}".format(proxy))


# your spider code

def get_html():
    # ....
    retry_count = 5
    proxy = get_proxy().get("proxy")
    print(proxy)
    while retry_count > 0:
        try:
            html = requests.get('http://www.baostock.com/',
                                proxies={"http": "http://{}".format(proxy), "https": "https://{}".format(proxy)})
            # 使用代理访问
            return html
        except Exception:
            retry_count -= 1
            # 删除代理池中代理
            delete_proxy(proxy)
    return None


if __name__ == '__main__':
    print(get_html())
