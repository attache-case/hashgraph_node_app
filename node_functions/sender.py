import asyncio
from random import random
import socket
from time import sleep
# from tqdm import tqdm

from node_functions.utils import msg_processor
from node_functions.utils import msg_composer
# from node_functions.utils import msg_parser

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


"""
Client Side
"""

async def tcp_client(message, addr, port, loop,
                          encoded=True, max_retry=10):
    retry_cnt = 0
    while retry_cnt < max_retry:
        try:
            reader, writer = \
                await asyncio.open_connection(addr, port,
                                              loop=loop)
            break
        except:
            r = random() * 3
            print(f'open_coneection({addr}:{port}) failed. sleep {r} sec.')
            await asyncio.sleep(r)
            retry_cnt += 1
    if retry_cnt >= max_retry:
        print(f'open_coneection({addr}:{port}) retry exceeded.')
        return

    # print('Send: %r' % message)
    if encoded:
        writer.write(message)
    else:
        writer.write(message.encode())
    writer.write_eof()


    full_data = await reader.read(-1)  # receive until EOF (* sender MUST send EOF at the end.)
    print(f'Received: {full_data}')  # if needed -> data.decode()

    print('Close the socket')
    writer.close()

def send_init_info_asyncio(info, dest_ip, dest_port):
    msg_tuple = msg_composer.compose_init_msg(info)
    msg = msg_processor.create_msg(*msg_tuple)
    
    loop = asyncio.get_event_loop()
    loop.run_until_complete(tcp_client(msg, dest_ip, dest_port, loop))
    loop.close()