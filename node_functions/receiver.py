import socket
# from tqdm import tqdm

from node_functions.utils import msg_processor
from node_functions.utils import msg_parser


def receive_tell_msg(listen_ip='0.0.0.0', listen_port=50008):
    """
    マネージャーノードから他のノードの情報がTELLされるのを待ち、受信する

    Returns:
        info: dict of objects
            他のノードと通信を開始するのに必要な情報
    """
    # 
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        # IPアドレスとポートを指定してbindする
        # FWやセキュリティポリシーで解放されているIP/Portにするべきである
        s.bind((listen_ip, listen_port))
        # 接続待ち受け
        s.listen(10)

        received_all_client_info = False
        # connectionするまで待つ
        while True:
            # 接続
            conn, addr = s.accept()
            header, payload = msg_processor.recv_msg(conn)
            msg_type = msg_parser.parse_msg_sub_header(header)['msg_type']
            if msg_type == 'TELL':
                info = msg_parser.parse_tell_msg(payload)
                received_all_client_info = True
                conn.sendall(b'OK: Received your TELL.')
            else:
                info = {}
                conn.sendall(b'NG: Only receiving your TELL now.')
            # TELLを受信し処理が終わったら、待ち受けをやめる
            if received_all_client_info is True:
                break
            
    return info
