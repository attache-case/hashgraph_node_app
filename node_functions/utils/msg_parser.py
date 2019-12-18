from pickle import dumps, loads


def load_object(payload):
    return loads(payload)


def parse_msg_sub_header(msg_sub_header_str):
    parse_result = {
        'msg_type': msg_sub_header_str[:4]
    }
    return parse_result


# def parse_init_msg(payload):
#     d = load_object(payload)
#     return d['pub_ip'], d['pk']

def parse_tell_msg(payload):
    d = load_object(payload)
    return d
