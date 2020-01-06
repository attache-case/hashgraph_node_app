# -*- coding: utf-8 -*-

import asyncio
from collections import namedtuple, defaultdict
from copy import deepcopy
import hashlib
from itertools import zip_longest
from functools import reduce
from pickle import dumps, loads
from queue import Queue
from random import choice, random
import socket
import sys
import threading
from time import time, sleep

from pysodium import (crypto_sign_keypair, crypto_sign, crypto_sign_open,
                      crypto_sign_detached, crypto_sign_verify_detached,
                      crypto_generichash)

from model.utils import bfs, toposort, randrange

import sys, signal

TX_LIMIT_PER_NODE = 500

"""
Original Error
"""

class ENDHashgraph(Exception):
    """実験終了を知らせる例外クラス"""
    pass


"""
Client Side
"""


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


"""
Node
"""

C = 6


def transform_info_to_tuple(my_kp, info):
    pub_ips = info['nodes']
    pk_dict = info['node_pks']
    shard_ip_belong = info['shard_belong']
    shard_pk_belong = {pk_dict[ip]:shard_ip_belong[ip] for ip in pub_ips}
    pks = [pk_dict[pub_ip] for pub_ip in pub_ips]
    # pk2pubip = {pk: pub_ip for pub_ip, pk in pk_dict.items()}
    # TBD: now, network should be ips or pks of nodes
    network = {pk: pub_ip for pub_ip, pk in pk_dict.items()}
    stake = {pk: 1 for pk in pks}
    balance = {pk: 1000 for pk in pks}
    info_tuple = (
        my_kp,
        info['n_shards'],
        network,
        pk_dict,  # r_network
        shard_ip_belong,
        shard_pk_belong,
        info['n_nodes'],
        stake,
        balance,
        info['interval_s']
    )
    return info_tuple


def majority(it):
    hits = [0, 0]
    for s, x in it:
        hits[int(x)] += s
    if hits[0] > hits[1]:
        return False, hits[0]
    else:
        return True, hits[1]


Event = namedtuple('Event', 'd p t c s') # data, parent hashes, event timestamp, creator ID(pubkey), signature
class Trilean:
    false = 0
    true = 1
    undetermined = 2


class Node:
    def __init__(self, kp, n_shards,
                 network, r_network,
                 shard_ip_belong, shard_pk_belong,
                 n_nodes, stake,
                 balance, interval_s):
        self.lock = threading.Lock()

        self.interval_s = interval_s

        self.pk, self.sk = kp
        self.n_shards = n_shards
        self.my_shard_id = shard_pk_belong[self.pk]
        self.network = network  # {pk -> pub_ip} dict
        self.r_network = r_network  # {pub_ip -> pk} dict
        self.n = n_nodes  # have to be according to shard
        self.stake = stake  # have to be according to shard
        self.tot_stake = sum(stake.values())  # have to be according to shard
        self.min_s = 2 * self.tot_stake / 3  # min stake amount

        self.shard_ip_belong = shard_ip_belong
        self.shard_pk_belong = shard_pk_belong

        self.log_sync_prep_process_time = []
        self.log_nw_out_bytes = []
        self.log_nw_in_bytes = []
        self.log_nw_io_time = []
        self.log_ask_sync_process_time = []
        self.log_sync_post_process_time = []
        self.log_sync_all_time = []

        # {member-pk => int(balance value)}
        self.balance = deepcopy(balance)

        self.flg = False # CHANGED: for debug

        # {event-hash => event}: this is the hash graph
        self.hg = {}
        # event-hash: latest event from me
        self.head = None
        # {event-hash => round-num}: assigned round number of each event
        self.round = {}
        # {event-hash}: events for which final order remains to be determined
        self.tbd = set()
        # [event-hash]: final order of the transactions
        self.transactions = []
        # {event-hash => index of the event in the final transactions list}
        self.idx = {}
        # {round-num}: rounds where famousness is fully decided
        self.consensus = set()
        # CHANGED: {event-hash => consensus-timestamp}: consensus timestamp of the event
        self.consensusts = {}
        # CHANGED: {event-hash => consensus-time}: system time that the event reached consensus
        self.consensusat = {}
        # CHANGED: [consensus-time]: list of system time that events reached consensus
        self.consensusats = []
        # {event-hash => {event-hash => bool}}
        self.votes = defaultdict(dict)
        # {round-num => {member-pk => event-hash}}:
        self.witnesses = defaultdict(dict)
        self.famous = {}

        # {event-hash => int}: 0 or 1 + max(height of parents) (only useful for
        # drawing, it may move to viz.py)
        self.height = {}
        # {event-hash => {member-pk => event-hash}}: stores for each event ev
        # and for each member m the latest event from m having same round
        # number as ev that ev can see
        self.can_see = {}

        # transactions to be included in next event
        self.new_tx_list = []
        # transactions transferred to here and added to the event
        self.tx_list_to_be_sent = []

        # init first local event
        h, ev = self.new_event(None, ())
        self.add_event(h, ev)
        self.round[h] = 0
        self.witnesses[0][ev.c] = h
        self.can_see[h] = {ev.c: h}
        self.head = h

    def new_event(self, d, p):
        """Create a new event (and also return it's hash)."""

        assert p == () or len(p) == 2                   # 2 parents
        assert p == () or self.hg[p[0]].c == self.pk  # first exists and is self-parent
        assert p == () or self.hg[p[1]].c != self.pk  # second exists and not self-parent
        # TODO: fail if an ancestor of p[1] from creator self.pk is not an
        # ancestor of p[0]

        t = time()
        s = crypto_sign_detached(dumps((d, p, t, self.pk)), self.sk)
        ev = Event(d, p, t, self.pk, s)

        return crypto_generichash(dumps(ev)), ev

    def is_valid_event(self, h, ev):
        try:
            crypto_sign_verify_detached(ev.s, dumps(ev[:-1]), ev.c)
        except ValueError:
            return False

        return (crypto_generichash(dumps(ev)) == h
                and (ev.p == ()
                     or (len(ev.p) == 2
                         and ev.p[0] in self.hg and ev.p[1] in self.hg
                         and self.hg[ev.p[0]].c == ev.c
                         and self.hg[ev.p[1]].c != ev.c)))

                         # TODO: check if there is a fork (rly need reverse edges?)
                         #and all(self.hg[x].c != ev.c
                         #        for x in self.preds[ev.p[0]]))))

    def add_event(self, h, ev):
        self.hg[h] = ev
        self.tbd.add(h)
        if ev.p == ():
            self.height[h] = 0
        else:
            self.height[h] = max(self.height[p] for p in ev.p) + 1

    def sync(self, pk, payload):
        """Update hg and return new event ids in topological order."""

        info = crypto_sign(dumps({c: self.height[h]
                for c, h in self.can_see[self.head].items()}), self.sk)
        msg = crypto_sign_open(self.network[pk](self.pk, info), pk)

        remote_head, remote_hg = loads(msg)
        new = tuple(toposort(remote_hg.keys() - self.hg.keys(),
                       lambda u: remote_hg[u].p))

        for h in new:
            ev = remote_hg[h]
            if self.is_valid_event(h, ev):
                self.add_event(h, ev)


        if self.is_valid_event(remote_head, remote_hg[remote_head]):
            h, ev = self.new_event(payload, (self.head, remote_head))
            # this really shouldn't fail, let's check it to be sure
            assert self.is_valid_event(h, ev)
            self.add_event(h, ev)
            self.head = h

        return new + (h,)

    def ask_sync(self, pk, info):
        """Respond to someone wanting to sync (only public method)."""

        # TODO: only send a diff? maybe with the help of self.height
        # TODO: thread safe? (allow to run while mainloop is running)

        cs = loads(crypto_sign_open(info, pk))

        subset = {h: self.hg[h] for h in bfs(
            (self.head,),
            lambda u: (p for p in self.hg[u].p
                       if self.hg[p].c not in cs or self.height[p] > cs[self.hg[p].c]))}
        msg = dumps((self.head, subset))
        return crypto_sign(msg, self.sk)

    def ancestors(self, c):
        while True:
            yield c
            if not self.hg[c].p:
                return
            c = self.hg[c].p[0]

    def maxi(self, a, b):
        if self.higher(a, b):
            return a
        else:
            return b

    def _higher(self, a, b):
        for x, y in zip_longest(self.ancestors(a), self.ancestors(b)):
            if x == b or y is None:
                return True
            elif y == a or x is None:
                return False

    def higher(self, a, b):
        return a is not None and (b is None or self.height[a] >= self.height[b])


    def divide_rounds(self, events):
        """Restore invariants for `can_see`, `witnesses` and `round`.

        :param events: topologicaly sorted sequence of new event to process.
        """

        for h in events:
            ev = self.hg[h]
            if ev.p == ():  # this is a root event
                self.round[h] = 0
                self.witnesses[0][ev.c] = h
                self.can_see[h] = {ev.c: h}
            else:
                r = max(self.round[p] for p in ev.p)

                # recurrence relation to update can_see
                p0, p1 = (self.can_see[p] for p in ev.p)
                self.can_see[h] = {c: self.maxi(p0.get(c), p1.get(c))
                                   for c in p0.keys() | p1.keys()}


                # count distinct paths to distinct nodes
                hits = defaultdict(int)
                for c, k in self.can_see[h].items():
                    if self.round[k] == r:
                        for c_, k_ in self.can_see[k].items():
                            if self.round[k_] == r:
                                hits[c_] += self.stake[c]
                # check if i can strongly see enough events
                if sum(1 for x in hits.values() if x > self.min_s) > self.min_s:
                    self.round[h] = r + 1
                else:
                    self.round[h] = r
                self.can_see[h][ev.c] = h
                if self.round[h] > self.round[ev.p[0]]:
                    self.witnesses[self.round[h]][ev.c] = h

    def decide_fame(self):
        max_r = max(self.witnesses)
        max_c = 0
        while max_c in self.consensus:
            max_c += 1

        # helpers to keep code clean
        def iter_undetermined(r_):
            for r in range(max_c, r_):
                if r not in self.consensus:
                    for w in self.witnesses[r].values():
                        if w not in self.famous:
                            yield r, w

        def iter_voters():
            for r_ in range(max_c + 1, max_r + 1):
                for w in self.witnesses[r_].values():
                    yield r_, w

        done = set()

        for r_, y in iter_voters():

            hits = defaultdict(int)
            for c, k in self.can_see[y].items():
                if self.round[k] == r_ - 1:
                    for c_, k_ in self.can_see[k].items():
                        if self.round[k_] == r_ - 1:
                            hits[c_] += self.stake[c]
            s = {self.witnesses[r_ - 1][c] for c, n in hits.items()
                 if n > self.min_s}

            for r, x in iter_undetermined(r_):
                if r_ - r == 1:
                    self.votes[y][x] = x in s
                else:
                    v, t = majority((self.stake[self.hg[w].c], self.votes[w][x]) for w in s)
                    if (r_ - r) % C != 0:
                        if t > self.min_s:
                            self.famous[x] = v
                            done.add(r)
                        else:
                            self.votes[y][x] = v
                    else:
                        if t > self.min_s:
                            self.votes[y][x] = v
                        else:
                            # the 1st bit is same as any other bit right?
                            self.votes[y][x] = bool(self.hg[y].s[0] // 128)

        new_c = {r for r in done
                 if all(w in self.famous for w in self.witnesses[r].values())}
        self.consensus |= new_c
        return new_c


    def find_order(self, new_c):
        to_int = lambda x: int.from_bytes(self.hg[x].s, byteorder='big')
        finals = []

        for r in sorted(new_c):
            f_w = {w for w in self.witnesses[r].values() if self.famous[w]}
            white = reduce(lambda a, b: a ^ to_int(b), f_w, 0)
            ts = {}
            seen = set()
            for x in bfs(filter(self.tbd.__contains__, f_w),
                         lambda u: (p for p in self.hg[u].p if p in self.tbd)):
                c = self.hg[x].c
                s = {w for w in f_w if c in self.can_see[w]
                                    and self.higher(self.can_see[w][c], x)}
                if sum(self.stake[self.hg[w].c] for w in s) > self.tot_stake / 2:
                    self.tbd.remove(x)
                    seen.add(x)
                    times = []
                    for w in s:
                        a = w
                        while (c in self.can_see[a]
                               and self.higher(self.can_see[a][c], x)
                               and self.hg[a].p):
                            a = self.hg[a].p[0]
                        times.append(self.hg[a].t)
                    times.sort()
                    ts[x] = .5*(times[len(times)//2]+times[(len(times)+1)//2])
            final = sorted(seen, key=lambda x: (ts[x], white ^ to_int(x)))
            for i, x in enumerate(final):
                self.idx[x] = i + len(self.transactions)
                self.consensusats.append(time()) # CHANGED: system time of the consensus of that event
                self.consensusts[x] = ts[x] # add if need to keep and call with hash later
                self.consensusat[x] = time() # add if need to keep and call with hash later
            self.transactions += final
            finals += final
        if self.consensus:
            # print(self.consensus)
            pass
        return finals


    def execute_transactions(self, finals):
        for x in finals:
            ev = self.hg[x]
            d = ev.d
            if not d:
                continue
            for tx in d:
                if tx['type'] == 'send':
                    result = 'ok'
                    amount = tx['amount']
                    sender = tx['sender']
                    receiver = tx['receiver']
                    if amount > 0 and self.balance[sender] >= amount:
                        self.balance[sender] -= amount
                        self.balance[receiver] += amount
                    else:
                        result = 'ng'
    

    def randomly_add_tx_to_new_tx_list(self, th_r):
        r = random()
        if r < th_r:
            c = tuple(self.network.keys() - {self.pk})[randrange(self.n - 1)]
            d = {
                "type": "send",
                "sender": self.pk,
                "receiver": c,
                "amount": randrange(100)
            }
            self.new_tx_list.append(d)

    def read_out_new_tx_list_to_tx_list_to_be_sent(self):
        self.tx_list_to_be_sent = deepcopy(self.new_tx_list)
        self.new_tx_list = []

    async def sync_loop(self, loop):
        """Update hg and return new event ids in topological order."""

        n_txs_to_be_reached = self.n*TX_LIMIT_PER_NODE
        print(f'run until {n_txs_to_be_reached} TXs reach consensus.')
        print(f'sync interval is set to {self.interval_s}')
        tx_done_percent = 0
        conn_fail_count = 0
        while True:
            conn_fail_count = 0
            t1 = time()
            t_prep_1 = time()
            if len(self.transactions) < n_txs_to_be_reached:
                self.randomly_add_tx_to_new_tx_list(0.1)
                self.read_out_new_tx_list_to_tx_list_to_be_sent()
            else:
                self.tx_list_to_be_sent = []
                break  # MUST DELETE
            payload = deepcopy(self.tx_list_to_be_sent)

            # pick a random node to sync with but not me
            c = tuple(self.network.keys() - {self.pk})[randrange(self.n - 1)]
            
            # new = self.sync(c, payload)
            
            info = crypto_sign(dumps({c: self.height[h]
                    for c, h in self.can_see[self.head].items()}), self.sk)
            
            t_prep_2 = time()
            t_nw_1 = time()

            try:
                reader, writer = \
                    await asyncio.open_connection(self.network[c], 50020,
                                                  loop=loop)
                print(f'open_coneection({self.network[c]}:{50020}) OK.')
            except:
                conn_fail_count += 1
                if conn_fail_count > 10:
                    await asyncio.sleep(min(0.2, conn_fail_count/100))
                if conn_fail_count > 100:
                    print(f'Conn fail > 100. break.')
                    break
                print(f'open_coneection({self.network[c]}:{50020}) NG.')
                continue
            
            try:
                # print('Send: %r' % info)
                writer.write(info)
                writer.write_eof()

                ret_info = await reader.read(-1)  # receive until EOF (* sender MUST send EOF at the end.)
                # print(f'Received: {ret_info}')  # if needed -> data.decode()

                # print('Close the socket')
                writer.close()
            except Exception as e:
                print(e)
                continue

            t_nw_2 = time()
            t_post_1 = time()
            
            msg = crypto_sign_open(ret_info, c)

            remote_head, remote_hg = loads(msg)
            new = tuple(toposort(remote_hg.keys() - self.hg.keys(),
                        lambda u: remote_hg[u].p))

            for h in new:
                ev = remote_hg[h]
                if self.is_valid_event(h, ev):
                    self.add_event(h, ev)


            if self.is_valid_event(remote_head, remote_hg[remote_head]):
                h, ev = self.new_event(payload, (self.head, remote_head))
                # this really shouldn't fail, let's check it to be sure
                assert self.is_valid_event(h, ev)
                self.add_event(h, ev)
                self.head = h

            new = new + (h,)

            # print(f'Length of new: {len(new)}')
            
            self.divide_rounds(new)

            new_c = self.decide_fame()
            finals = self.find_order(new_c)
            self.execute_transactions(finals)

            t_post_2 = time()
            
            t2 = time()
            t_elapsed = t2 - t1
            if t_elapsed < self.interval_s:
                await asyncio.sleep(self.interval_s - t_elapsed)
            self.log_sync_all_time.append(t2-t1)
            self.log_sync_prep_process_time.append(t_prep_2-t_prep_1)
            self.log_nw_io_time.append(t_nw_2-t_nw_1)
            self.log_sync_post_process_time.append(t_post_2-t_post_1)
            self.log_nw_out_bytes.append(sys.getsizeof(info))
            self.log_nw_in_bytes.append(sys.getsizeof(msg))
            l_txs = len(self.transactions)
            if l_txs * 100 // n_txs_to_be_reached > tx_done_percent:
                tx_done_percent = l_txs * 100 // n_txs_to_be_reached
                print(f'TXs reached consensus: {l_txs} ... {tx_done_percent}%')

        print(f'wait 60 secs to shutdown')
        await asyncio.sleep(60)
        raise ENDHashgraph()


    async def ask_sync_loop(self, loop, sock):
        """Respond to someone wanting to sync (only public method)."""

        # TODO: only send a diff? maybe with the help of self.height
        # TODO: thread safe? (allow to run while mainloop is running)
        while True:
            new_socket, (remote_host, remote_remport) = await loop.sock_accept(sock)
            new_socket.setblocking(False)
            # print('[FD:{}]Accept:{}:{}'.format(new_socket.fileno(), remote_host, remote_remport))
            asyncio.ensure_future(self.recv_send(loop, new_socket, remote_host, remote_remport))

    async def recv_send(self, loop, sock, remote_host, remote_remport):
        try:
            remote_host, remote_remport = sock.getpeername()
            # print('[FD:{}]Client:{}:{}'.format(sock.fileno(), remote_host, remote_remport))

            t1 = time()
            full_data = b''
            while True:
                data = await loop.sock_recv(sock, 512)
                if data == b'':
                    # print('[FD:{}]Recv:EOF'.format(sock.fileno()))
                    cs = loads(crypto_sign_open(full_data, self.r_network[remote_host]))

                    subset = {h: self.hg[h] for h in bfs(
                        (self.head,),
                        lambda u: (p for p in self.hg[u].p
                                if self.hg[p].c not in cs or self.height[p] > cs[self.hg[p].c]))}
                    msg = dumps((self.head, subset))
                    msg_ret = crypto_sign(msg, self.sk)
                    # print(f'Return subset of length: {len(subset)}')
                    await loop.sock_sendall(sock, msg_ret)
                    sock.close()
                    break

                # print('[FD:{}]Recv:{}'.format(sock.fileno(), data))
                full_data += data
            t2 = time()
            self.log_ask_sync_process_time.append(t2-t1)
        except:
            print(f'Exception while recv_send()')
            sock.close()

    
    def main_asyncio(self):
        event_loop = asyncio.SelectorEventLoop()
        asyncio.set_event_loop(event_loop)
        server_sock = create_server_socket('0.0.0.0', 50020)

        gather_list = [
            self.ask_sync_loop(event_loop, server_sock)
        ]
        # for shard_id in range(self.n_shards):
        #     gather_list.append(tcp_echo_client(msg_init, addr, 50010, event_loop))
        gather_list.append(self.sync_loop(event_loop))
        gather_tuple = tuple(gather_list)
        try:
            event_loop.run_until_complete(
                asyncio.gather(
                    *gather_tuple
                )
            )
        except ENDHashgraph:
            event_loop.close()
            server_sock.close()
        except KeyboardInterrupt:
            event_loop.close()
            server_sock.close()

        finality_sec = []
        for x in self.transactions:
            finality = self.consensusat[x] - self.hg[x].t
            finality_sec.append(finality)
        # print(finality_sec)

        log_info = {
            'n_nodes': self.n,
            'n_shards': self.n_shards,
            'interval_s': self.interval_s,
            'len_txs': len(self.transactions),
            'log_sync_all_time': self.log_sync_all_time,
            'log_sync_prep_process_time': self.log_sync_prep_process_time,
            'log_nw_io_time': self.log_nw_io_time,
            'log_sync_post_process_time': self.log_sync_post_process_time,
            'log_nw_out_bytes': self.log_nw_out_bytes,
            'log_nw_in_bytes': self.log_nw_in_bytes,
            'log_ask_sync_process_time': self.log_ask_sync_process_time,
            'finality_sec': finality_sec
        }

        t_now = int(time())
        import pickle
        fname = f'{log_info["n_nodes"]}_{log_info["n_shards"]}_{log_info["interval_s"]}_{t_now}.binaryfile'
        with open(f'./logs/{fname}', 'wb') as f:
            pickle.dump(log_info, f)
        print(f'log binaryfile created: {fname}')
    

    def test_c(self):
        print(C)


def test(n_nodes, n_turns):
    kps = [crypto_sign_keypair() for _ in range(n_nodes)]
    network = {}
    stake = {kp[0]: 1 for kp in kps}
    balance = {kp[0]: 1000 for kp in kps}
    nodes = [Node(kp, network, n_nodes, stake, balance) for kp in kps]
    for n in nodes:
        network[n.pk] = n.ask_sync
    mains = [n.main() for n in nodes]
    for m in mains:
        next(m)
    for i in range(n_turns):
        r = randrange(n_nodes)
        # print('working node: %i, event number: %i' % (r, i))
        next(mains[r])
    return nodes


if __name__ == '__main__':
    nodes = test(4, 1000)