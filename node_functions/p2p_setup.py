# -*- coding: utf-8 -*-
import asyncio
from random import random
import socket
import threading
from time import time
import queue
# from tqdm import tqdm

# BYTE_EOF = b'\x04'

try:
    from node_functions.utils import msg_composer
    from node_functions.utils import msg_processor
    from node_functions.utils import msg_parser
except ModuleNotFoundError:
    from utils import msg_composer
    from utils import msg_processor
    from utils import msg_parser


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


"""
Original Error
"""

class P2PSetupTimeUpError(Exception):
    """P2Pの導通確認の時間切れを知らせる例外クラス"""
    pass

class P2PReceivedAll(Exception):
    """P2Pの導通確認の終了を知らせる例外クラス"""
    pass


"""
Client Side
"""

async def tcp_echo_client(message, addr, port, loop,
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

    # full_data = b''
    # while True:
    #     data = await reader.read(512)  # 512
    #     if data == b'': # if b'\x04'(Ctrl-D) is sent, can detect this maybe.
    #         break
    #     else:
    #         full_data += data
    full_data = await reader.read(-1)  # receive until EOF (* sender MUST send EOF at the end.)
    msg_sub_header_str, full_payload = msg_processor.split_fully_rcvd_msg(full_data)
    msg_type = msg_parser.parse_msg_sub_header(msg_sub_header_str)['msg_type']
    # print(f'Received: {full_data}')  # if needed -> data.decode()

    # print('Close the socket')
    writer.close()
    sendable_ips.append(addr)
    print(f'Sendable nodes: {len(sendable_ips)}')


"""
Server Side
"""

def create_server_socket(addr, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.setblocking(False)
    sock.bind((addr, port))
    sock.listen(256)
    print('Server Run Port:{}'.format(port))
    return sock


async def accept(loop, sock, n_nodes_to_recv):
    print('Ready For Accept')

    while True:
        new_socket, (remote_host, remote_remport) = await loop.sock_accept(sock)
        new_socket.setblocking(False)
        print('[FD:{}]Accept:{}:{}'.format(new_socket.fileno(), remote_host, remote_remport))
        asyncio.ensure_future(recv_send(loop, new_socket, remote_host, remote_remport, n_nodes_to_recv))


async def recv_send(loop, sock, remote_host, remote_remport, n_nodes_to_recv):
    remote_host, remote_remport = sock.getpeername()
    print('[FD:{}]Client:{}:{}'.format(sock.fileno(), remote_host, remote_remport))

    full_data = b''
    while True:
        data = await loop.sock_recv(sock, 512)
        if data == b'':
            print('[FD:{}]Recv:EOF'.format(sock.fileno()))
            msg_sub_header_str, full_payload = msg_processor.split_fully_rcvd_msg(full_data)
            msg_type = msg_parser.parse_msg_sub_header(msg_sub_header_str)['msg_type']
            if msg_type == 'INIT':
                msg_ret = b'OK: received your INIT.'
                pub_ip, pk = msg_parser.parse_init_msg(full_payload)
                new_addr2pub_ip[remote_host] = pub_ip
                receivable_ips.append(pub_ip)
            else:
                msg_ret = b'NG: your message was not INIT.'
            await loop.sock_sendall(sock, msg_ret)
            sock.close()
            break

        # print('[FD:{}]Recv:{}'.format(sock.fileno(), data))
        full_data += data
        # await loop.sock_sendall(sock, data)
    
    print(f'Receivable nodes: {len(receivable_ips)}')
    if len(receivable_ips) >= n_nodes_to_recv:
        raise P2PReceivedAll()

"""
Loops
"""

async def p2p_setup_timer(t_limit, loop):
    await asyncio.sleep(t_limit)
    raise P2PSetupTimeUpError()


"""
Main Function
"""

def p2p_setup_main(my_info, info):
    try:
        result_tuple = (None, None)
        new_info = {}
        new_addr2pub_ip = info['addr2pub_ip']
        addrs = list(set(info['nodes']) - {my_info['pub_ip']})
        msg_init = msg_processor.create_msg(
            *msg_composer.compose_init_msg(my_info)
        )
        n_nodes_to_recv = info['n_nodes'] - 1
        # print('INIT message to send:')
        # print(msg_init)

    except:
        raise

    event_loop = asyncio.SelectorEventLoop()
    asyncio.set_event_loop(event_loop)
    server_sock = create_server_socket('0.0.0.0', 50010)

    gather_list = [
        accept(event_loop, server_sock, n_nodes_to_recv),
        p2p_setup_timer(60, event_loop)
    ]
    for addr in addrs:
        gather_list.append(tcp_echo_client(msg_init, addr, 50010, event_loop))
    gather_tuple = tuple(gather_list)
    try:
        event_loop.run_until_complete(
            asyncio.gather(
                *gather_tuple
                # accept(event_loop, server_sock),
                # tcp_echo_client('test', 7778, event_loop)
            )
        )
    except P2PSetupTimeUpError:
        event_loop.close()
        server_sock.close()
    except P2PReceivedAll:
        print(f'received from all nodes. end setup.')
        event_loop.close()
        server_sock.close()
    except KeyboardInterrupt:
        event_loop.close()
        server_sock.close()
    
    if len(sendable_ips) == (info['n_nodes'] - 1):
        send_status = 'A'
    else:
        send_status = 'B'
    
    if len(receivable_ips) == (info['n_nodes'] - 1):
        receive_status = 'A'
    else:
        receive_status = 'B'
    
    result_tuple = (send_status, receive_status)

    new_info['n_nodes'] = info['n_nodes']
    new_info['nodes'] = list(set(sendable_ips)&set(receivable_ips)|{my_info['pub_ip']})
    new_info['addr2pub_ip'] = new_addr2pub_ip
    new_info['node_pks'] = info['node_pks']
    new_info['shard_belong'] = info['shard_belong']
    new_info['n_shards'] = info['n_shards']
    new_info['interval_s'] = info['interval_s']
    
    return result_tuple, new_info


"""
Main
"""

if __name__ == '__main__':
    event_loop = asyncio.SelectorEventLoop()
    asyncio.set_event_loop(event_loop)
    server_sock = create_server_socket('', 7777)

    gather_list = [
        accept(event_loop, server_sock)
    ]
    for port in range(7778, 7783):
        gather_list.append(tcp_echo_client(f'test{port}', port, event_loop))
    gather_tuple = tuple(gather_list)
    try:
        event_loop.run_until_complete(
            asyncio.gather(
                *gather_tuple
                # accept(event_loop, server_sock),
                # tcp_echo_client('test', 7778, event_loop)
            )
        )
    except KeyboardInterrupt:
        event_loop.close()
        server_sock.close()