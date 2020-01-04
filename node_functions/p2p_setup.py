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
Client Side
"""

async def tcp_echo_client(message, addr, port, loop, encoded=True):
    retry_cnt = 0
    while retry_cnt < 10:
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
    if retry_cnt >= 10:
        print(f'open_coneection({addr}:{port}) retry exceeded.')
        return

    print('Send: %r' % message)
    if encoded:
        writer.write(message)
    else:
        writer.write(message.encode())

    full_data = b''
    while True:
        data = await reader.read(512)
        if data == b'': # if b'\x04'(Ctrl-D) is sent, can detect this maybe.
            break
        else:
            full_data += data
    # full_data = await reader.read(-1)  # receive until EOF (* sender MUST send EOF at the end.)
    msg_sub_header_str, full_payload = msg_processor.split_fully_rcvd_msg(full_data)
    msg_type = msg_parser.parse_msg_sub_header(msg_sub_header_str)['msg_type']
    print(f'Received: {msg_type}::{full_payload}')  # if needed -> data.decode()

    print('Close the socket')
    writer.close()


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


async def accept(loop, sock):
    print('Ready For Accept')

    while True:
        new_socket, (remote_host, remote_remport) = await loop.sock_accept(sock)
        new_socket.setblocking(False)
        print('[FD:{}]Accept:{}:{}'.format(new_socket.fileno(), remote_host, remote_remport))
        asyncio.ensure_future(recv_send(loop, new_socket))


async def recv_send(loop, sock):
    remote_host, remote_remport = sock.getpeername()
    print('[FD:{}]Client:{}:{}'.format(sock.fileno(), remote_host, remote_remport))

    while True:
        data = await loop.sock_recv(sock, 512)
        if data == b'':
            print('[FD:{}]Recv:EOF'.format(sock.fileno()))
            await loop.sock_sendall(sock, b'')
            sock.close()
            break

        print('[FD:{}]Recv:{}'.format(sock.fileno(), data))
        # await loop.sock_sendall(sock, data)
        await loop.sock_sendall(sock, data)

"""
Loops
"""


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
        print('INIT message to send:')
        print(msg_init)

    except:
        raise

    event_loop = asyncio.SelectorEventLoop()
    asyncio.set_event_loop(event_loop)
    server_sock = create_server_socket('0.0.0.0', 50010)

    gather_list = [
        accept(event_loop, server_sock)
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
    except KeyboardInterrupt:
        event_loop.close()
        server_sock.close()
    
    result_tuple = (send_status, receive_status)

    new_info['n_nodes'] = info['n_nodes']
    new_info['nodes'] = list(set(sendable_ips)&set(receivable_ips))
    new_info['addr2pub_ip'] = new_addr2pub_ip
    new_info['node_pks'] = info['node_pks']
    
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