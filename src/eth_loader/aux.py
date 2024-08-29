import base64 as b64

def to_b64(s: str):
    """
    Convert string to base64
    :param s: string to convert
    :return:
    """
    return b64.b64encode(s.encode("utf-8")).decode("utf-8")


def from_b64(s: str):
    """
    Convert base64 to string
    :param s: base64 to convert
    :return:
    """
    return b64.b64decode(s.encode("utf-8")).decode("utf-8")


def escape_sql(s: str):
    """
    Escape a string for sql
    :param s: string to escape
    :return:
    """
    return s.replace("'", "''")