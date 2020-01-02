import configparser
from pickle import dumps, loads
from pysodium import (crypto_sign_keypair, crypto_sign, crypto_sign_open,
                      crypto_sign_detached, crypto_sign_verify_detached,
                      crypto_generichash)
import requests
import socket


config = configparser.ConfigParser()
try:
    config.read('/home/ec2-user/hashgraph_node_app/node_functions/utils/config.ini')
    assert(int(config['DEFAULT']['MSG_BUF_LEN'])>0)
except:
    try:
        config = configparser.ConfigParser()
        config.read('./node_functions/utils/config.ini')
        assert(int(config['DEFAULT']['MSG_BUF_LEN'])>0)
    except:
        raise
default_config = config['DEFAULT']

MSG_BUF_LEN = int(default_config['MSG_BUF_LEN'])
FIX_HEADER_LEN = int(default_config['FIX_HEADER_LEN'])


def encrypt_payload(payload, seckey):
    return crypto_sign(payload, seckey)


def decrypt_payload(payload, pubkey):
    return crypto_sign_open(payload, pubkey)


def create_msg(msg_type_str, payload):
    """
    Create message bytes with header

    Args:
        msg_type_str: string([A-Z]{4})
        payload: bytes
    
    Returns:
        msg: bytes
            |payload_len(20)|msg_type(4)|RESERVED(6)|payload(...)|
    """
    le_str = str(len(payload))
    fixed_le_str = le_str + (20 - len(le_str))*' '
    header_str = fixed_le_str + msg_type_str[:4]
    fixed_header_str = header_str + (FIX_HEADER_LEN - len(header_str))*' '

    header = fixed_header_str.encode('utf-8')
    
    msg = header + payload

    return msg


def recv_msg(conn):
    """
    Receive message with header

    Args:
        conn: socket object
    
    Returns:
        msg_sub_header_str: string
        full_payload: bytes
    """
    full_payload = b''
    new_msg = True
    while True:
        # データを受け取る
        data = conn.recv(MSG_BUF_LEN)
        if not data:
            full_payload = b''
            new_msg = True
            return None, None
        if new_msg:
            msg_header = data[:FIX_HEADER_LEN]
            msg_header_str= msg_header.decode('utf-8')
            payload_len = int(msg_header_str[:20])
            msg_sub_header_str = msg_header_str[20:]
            new_msg = False
            full_payload += data[FIX_HEADER_LEN:]
        else:
            full_payload += data
        
        if len(full_payload) == payload_len:
            return msg_sub_header_str, full_payload


def create_encrypted_msg(msg_type_str, payload, sk):
    encrypted_payload = encrypt_payload(payload, sk)
    return create_msg(msg_type_str, encrypted_payload)


def recv_encrypted_msg(conn, pk):
    msg_sub_header_str, encrypted_payload = recv_msg(conn)
    decrypted_payload = decrypt_payload(encrypted_payload, pk)
    return msg_sub_header_str, decrypted_payload
