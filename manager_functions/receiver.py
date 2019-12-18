import socket
from tqdm import tqdm

from manager_functions.utils import msg_processor
from manager_functions.utils import msg_parser

def collect_node_infos(n_nodes, n_listen=10, listen_ip='0.0.0.0', listen_port=50007):
    """
    所定のノード数までクライアントのパブリックIPと公開鍵の情報を収集する

    Args:
        n_nodes: int
        n_listen: int, default 10
        listen_ip: string, default '0.0.0.0'
        listen_port: int, default 50007

    Returns:
        info: dict of objects
            n_nodes: int
            nodes: list of string
                クライアントのパブリックIPv4アドレスのリスト
            addr2pub_ip: dict of string
                接続受付時に見えるaddrと伝えられたパブリックIPv4アドレスの対応表
            node_pks: dict of bytes
                クライアントから伝えられたクライアントの公開鍵
    """
    info = {}
    nodes = []
    addr2pub_ip = {}
    node_pks = {}

    n_nodes_recv = 0

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        # IPアドレスとポートを指定してbindする
        # FWやセキュリティポリシーで解放されているIP/Portにするべきである
        s.bind((listen_ip, listen_port))
        # 接続待ち受け
        s.listen(n_listen)

        print('Waiting for ' + str(n_nodes) + ' nodes to send its INIT info...')
        with tqdm(total=n_nodes) as pbar:
            while True:
                # 接続
                conn, addr = s.accept()
                header, payload = msg_processor.recv_msg(conn)
                msg_type = msg_parser.parse_msg_sub_header(header)['msg_type']
                if msg_type == 'INFO':
                    pub_ip, pk = msg_parser.parse_init_msg(payload)
                    nodes.append(pub_ip)
                    addr2pub_ip[addr[0]] = pub_ip
                    node_pks[pub_ip] = pk
                if n_nodes_recv < len(nodes):
                    pbar.update(len(nodes) - n_nodes_recv)
                    n_nodes_recv = len(nodes)
                # 所定のノード数分の情報が集まったら、待ち受けをやめる
                if len(nodes) >= n_nodes:
                    break
        
        info['n_nodes'] = n_nodes
        info['nodes'] = nodes
        info['addr2pub_ip'] = addr2pub_ip
        info['node_pks'] = node_pks
        return info
