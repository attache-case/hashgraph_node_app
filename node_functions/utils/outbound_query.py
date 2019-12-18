import requests

PUB_IP_CHECK_URL = 'http://ipecho.net/plain'  


def getPublicIp():
    """パブリックIPアドレスを返す。"""
    response = requests.get(PUB_IP_CHECK_URL) 
    if response.status_code == 200:
        return response.text
    else:
        return 'IPアドレスを取得できませんでした。'