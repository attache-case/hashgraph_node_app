import socket
import threading
from time import time
import queue
# from tqdm import tqdm

from node_functions.utils import msg_composer
from node_functions.utils import msg_processor
from node_functions.utils import msg_parser


send_queue = queue.Queue()
receive_set = set()

send_status = None
receive_status = None

sendable_ips = []
receivable_ips = []

new_addr2pub_ip = {}

# my_info['pk'] = my_pk
# my_info['pub_ip'] = pub_ip
# info['n_nodes'] = n_nodes
# info['nodes'] = nodes
# info['addr2pub_ip'] = addr2pub_ip
# info['node_pks'] = node_pks


def try_init(my_info, info, dest_port=50008):
    """
    他のノードに自身の情報をINITで伝え導通確認をする
    """
    send_status = 'A'
    fail_count = 0
    n_nodes = info['n_nodes']
    while True:
        next_queue = queue.Queue()
        while send_queue.empty() is False:
            addr = send_queue.get()
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((addr, dest_port))

                    # サーバにメッセージを送る
                    msg_tuple = msg_composer.compose_init_msg(my_info)
                    s.sendall(msg_processor.create_msg(*msg_tuple))

                    # サーバからの文字列を取得する。
                    data = s.recv(msg_processor.MSG_BUF_LEN)
                    if data:
                        sendable_ips.append(addr)
            except:
                next_queue.put(addr)
        if next_queue.empty():
            break
        elif fail_count < N//3:
            while next_queue.empty() is False:
                send_queue.put(next_queue.get())
            continue
        elif next_queue.qsize() < N//10:
            send_status = 'B'
            break
        else:
            send_status = 'C'
            break
    
    return


def listen_init(my_info ,info ,listen_ip='0.0.0.0', listen_port=50008):
    """
    他のノードからINITが来るのを待ち、受信する
    Returns:
        info: dict of objects
            他のノードと通信を開始するのに必要な情報
    """
    n_nodes = info['n_nodes']
    receive_status = 'A'
    t_listen_start_sec = time()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        # IPアドレスとポートを指定してbindする
        # FWやセキュリティポリシーで解放されているIP/Portにするべきである
        s.bind((listen_ip, listen_port))
        # 接続待ち受け
        s.listen(10)

        # connectionするまで待つ
        while True:
            # 接続
            conn, addr = s.accept()
            header, payload = msg_processor.recv_msg(conn)
            msg_type = msg_parser.parse_msg_sub_header(header)['msg_type']
            if msg_type == 'INIT':
                pub_ip, pk = msg_parser.parse_init_msg(payload)
                conn.sendall(b'OK: Received your INIT info.')
                new_addr2pub_ip[addr] = pub_ip
                receive_set.add(pub_ip)
            else:
                conn.sendall(b'NG: Only receiving your INIT info now.')

            if len(receive_set) == n_nodes:
                break
            else:
                t_listen_current_sec = time()
                t_listen_elapsed_sec = t_listen_current_sec - t_listen_start_sec
                if t_listen_elapsed_sec < 100:
                    continue
                elif len(receive_set) > 9*N//10:
                    receive_status = 'B'
                    break
                else:
                    receive_status = 'C'
                    break
    
    for addr in receive_set:
        receivable_ips.append(addr)
    
    return


def p2p_setup_main(my_info, info):
    result_tuple = (None, None)
    new_info = {}
    new_addr2pub_ip = info['addr2pub_ip']

    t_try_init = threading.Thread(target=try_init, args=(my_info, info))
    t_listen_init = threading.Thread(target=listen_init, args=(my_info, info))
    # print('==Thread Started==')
    t_try_init.start()
    t_listen_init.start()
    t_try_init.join()
    t_listen_init.join()

    result_tuple = (send_status, receive_status)

    new_info['n_nodes'] = info['n_nodes']
    new_info['nodes'] = list(set(sendable_ips)&set(receivable_ips))
    new_info['addr2pub_ip'] = new_addr2pub_ip
    new_info['node_pks'] = info['node_pks']

    return result_tuple, new_info
