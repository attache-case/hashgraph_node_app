from manager_functions import receiver
from manager_functions import sender

N_NODES = 50


if __name__ == "__main__":
    info = receiver.collect_node_infos(N_NODES)
    print("Got " + str(info['n_nodes']) + " nodes' info.")
    print("Summary of pubkeys:")
    print({ip: pk[:8] for ip, pk in info['node_pks'].items()})
    sender.tell_node_infos(info)