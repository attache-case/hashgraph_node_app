from pysodium import crypto_sign_keypair
import traceback

from node_functions import receiver
from node_functions import sender
from node_functions import p2p_setup
from node_functions.utils import outbound_query
from node_functions.utils import logger
from model import node


EC2_MANAGER_ELASTIC_IP = '52.199.141.89'
EC2_MANAGER_PORT = 50007

# いずれNodeオブジェクトに移す
my_kp = crypto_sign_keypair()
my_pk, my_sk = my_kp[0], my_kp[1]

pub_ip = outbound_query.getPublicIp()

n_nodes = None

nodes = None
addr2pub_ip = None
node_pks = None

is_monitor_node = False

if __name__ == "__main__":
    try:
        if is_monitor_node:
            logger.info('sample start.')

        my_info = {}
        my_info['pk'] = my_pk
        my_info['pub_ip'] = pub_ip
        sender.send_init_info(my_info, EC2_MANAGER_ELASTIC_IP, EC2_MANAGER_PORT)
        info = receiver.receive_tell_msg()
        print(info)

        setup_result, new_info = p2p_setup.p2p_setup_main(my_info, info)
        print(setup_result)
        print(new_info)

        info_tuple = node.transform_info_to_tuple(my_kp, new_info)
        n = node.Node(*info_tuple)
        n.main_asyncio(0.2)
        # n.test_c()

        if is_monitor_node:
            logger.info('sample end.')
    except Exception as e:
        if is_monitor_node:
            logger.error(traceback.format_exc())
            logger.error(e)
        print(e)