from pysodium import crypto_sign_keypair

from node_functions import receiver
from node_functions import sender
from node_functions.utils import outbound_query

EC2_MANAGER_ELASTIC_IP = '52.199.141.89'
EC2_MANAGER_PORT = 50007

# いずれNodeオブジェクトに移す
kp = crypto_sign_keypair()
my_pk, my_sk = kp[0], kp[1]

pub_ip = outbound_query.getPublicIp()

n_nodes = None

nodes = None
addr2pub_ip = None
node_pks = None


if __name__ == "__main__":
    info = {}
    info['pk'] = my_pk
    info['pub_ip'] = pub_ip
    sender.send_init_info(info, EC2_MANAGER_ELASTIC_IP, EC2_MANAGER_PORT)
    info = receiver.receive_tell_msg()
    print(info)