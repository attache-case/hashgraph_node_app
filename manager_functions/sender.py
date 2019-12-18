import socket
from tqdm import tqdm

from manager_functions.utils import msg_processor
from manager_functions.utils import msg_composer

def tell_node_infos(info, dest_port=50008):
    n_nodes = info['n_nodes']
    nodes = info['nodes']

    print('Sending TELL info to ' + str(n_nodes) + ' nodes...')
    with tqdm(total=n_nodes) as pbar:
        # クライアントノードにTELLで初期情報を伝える
        for node in nodes:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((node, dest_port))

                # メッセージを送る
                msg_tuple = msg_composer.compose_tell_msg(info)
                s.sendall(msg_processor.create_msg(*msg_tuple))

                # 文字列を取得する。
                data = s.recv(msg_processor.MSG_BUF_LEN)
                # 帰ってきた文字列を表示
                print(repr(data))
                pbar.update(1)