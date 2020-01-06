from random import random
import socket
from time import sleep
# from tqdm import tqdm

from node_functions.utils import msg_processor
from node_functions.utils import msg_composer

def send_init_info(info, dest_ip, dest_port):
    """
    マネージャノードに自身の情報を伝える
    """
    retry_cnt = 0
    while retry_cnt < 20:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(5)
                s.connect((dest_ip, dest_port))

                # サーバにメッセージを送る
                msg_tuple = msg_composer.compose_init_msg(info)
                s.sendall(msg_processor.create_msg(*msg_tuple))
                s.sendall(b'')

                # サーバからの文字列を取得する。
                data = s.recv(msg_processor.MSG_BUF_LEN)
                # 帰ってきた文字列を表示
                print(repr(data))
            break
        except socket.timeout:
            print(f'send INIT timeout')
            retry_cnt += 1
            sleep(5*random())
            continue
        except Exception as e:
            print(e)
            retry_cnt += 1
            sleep(5*random())
            continue