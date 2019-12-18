from pickle import dumps, loads


def dump_object(obj):
    return dumps(obj)


def compose_tell_msg(info):
    payload = dumps(info)
    msg_type_str = 'TELL'
    return msg_type_str, payload
