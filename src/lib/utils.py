import bson
from bson.json_util import dumps
import os

def bson_dump_pretty(obj):
    return bson.json_util.dumps(obj, sort_keys=True, indent=4, separators=(",", ": "))

def err(message):
    raise Exception(message)

def is_bool(value):
    return type(value) is bool

def _is_bool_test():
    if is_bool(True) == False:
        err("failed on True")
    if is_bool(False) == False:
        err("failed on False")

    if is_bool(0):
        err("failed on 0")
    if is_bool(1):
        err("failed on 1")
    if is_bool(-2):
        err("failed on -2")
    if is_bool(3.14):
        err("failed on 3.14")
    if is_bool("hello world"):
        err("failed on 'hello world'")
    if is_bool([]):
        err("failed on []")
    if is_bool({}):
        err("failed on \{\}")

def errIf(condition, message):
    if not is_bool(condition):
        err("Condition must be of type bool. Do not use truthy/falsey conditions.")
    if condition:
        err(message)

def verify(condition, message):
    if not is_bool(condition):
        err("Condition must be of type bool. Do not use truthy/falsey conditions.")
    if not condition:
        err(message)

def is_int(value):
    return type(value) is int

def is_numeric_int(value):
    try:
        int(value)
        return True
    except (ValueError, TypeError, OverflowError):
        return False

def is_float(value):
    return type(value) is float

def is_numeric_float(value):
    try:
        float(value)
        return True
    except (ValueError, TypeError, OverflowError):
        return False

def is_long(value):
    return type(value) is long

def is_numeric_long(value):
    try:
        long(value)
        return True
    except (ValueError, TypeError, OverflowError):
        return False

def is_complex(value):
    return type(value) is complex

def is_numeric_complex(value):
    try:
        complex(value)
        return True
    except (ValueError, TypeError, OverflowError):
        return False

def is_numeric(value):
    # todo include binary, octal, and hex strings
    # todo consider whether nan and inf should be excluded
    # in order of most likely to evaluate to true and short-circuit
    return is_numeric_complex(value) or is_numeric_float(value) or is_numeric_long(value) or is_numeric_int(value)

def _is_numeric_test():
    verify(is_numeric(0), "failed on 0")
    verify(is_numeric(1), "failed on 1")
    verify(is_numeric(-2), "failed on -2")
    verify(is_numeric(3.14), "failed on 3.14")
    verify(is_numeric(0x0123456789abcdef), "failed on 0x0123456789abcdef")
    verify(is_numeric(0b01001011), "failed on 0b01001011")
    verify(is_numeric("123"), "failed on '123'")
    verify(is_numeric("-456"), "failed on '-456'")
    verify(is_numeric("6.28"), "failed on '6.28'")
    verify(is_numeric(float("inf")), "failed on float('inf')")
    verify(is_numeric(float("-inf")), "failed on float('-inf')")

    errIf(is_numeric("hello world"), "failed on 'hello world'")
    errIf(is_numeric("123qwe"), "failed on '123qwe")
    errIf(is_numeric("asd456"), "failed on 'asd456'")
    errIf(is_numeric(None), "failed on None")
    errIf(is_numeric([]), "failed on []")
    errIf(is_numeric({}), "failed on \{\}")
    # errIf(is_numeric(float('nan')), "failed on float('nan')")

def is_str(value):
    return type(value) is str or type(value) is unicode

def dir_ensure_exists(directory, recurse=False):
    # todo consider ensuring a trailing slash
    verify(is_str(directory), "directory must be of type str")
    verify(is_bool(recurse), "recurse flag must be of type bool")

    if not os.path.isdir(directory):
        if recurse:
            os.makedirs(directory)
        else:
            os.mkdir(directory)

def is_list(value):
    return type(value) is list

def _is_list_test():
    verify(is_list([]), "failed on []")

    errIf(is_list(0), "failed on 0")
    errIf(is_list(1), "failed on 1")
    errIf(is_list(True), "failed on True")
    errIf(is_list({}), "failed on \{\}")

def is_dict(value):
    return type(value) is dict

def _is_dict_test():
    verify(is_dict({}), "failed on \{\}")

    errIf(is_dict(0), "failed on 0")
    errIf(is_dict([]), "failed on []")
    errIf(is_dict(lambda: 0), "failed on lambda: 0")
    errIf(is_dict(True), "failed on True")
    errIf(is_dict(-3.14), "failed on -3.14")

def traverse_json_object(json_obj, callback):
    '''
    recursively traverses a dictionary which has json-like structure
    recurses on list and dict typed values
    calls callback on all other values, treating them as leaf-nodes
    returns None

    callback must take two parameters, key_path and value
        key_path is an array of keys, in order of recursive traversal
        value current value being explored
    '''

    def _recurse(obj, path=[]):
        if type(obj) == list:
            for i, value in enumerate(obj):
                _recurse(value, path+[i])
        elif type(obj) == dict:
            for key, value in obj.items():
                _recurse(value, path+[key])
        else:
            callback(path, obj)

    _recurse(json_obj)

if __name__ == '__main__':
    _is_bool_test()
    _is_numeric_test()
    _is_list_test()
    _is_dict_test()
    print("all tests passed")
