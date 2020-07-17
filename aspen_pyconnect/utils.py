import hashlib
from time import mktime
from dateutil import parser


def iso_to_js_dt(iso_datetime):
    decoded_aspen_time = parser.parse(iso_datetime)
    js_time = int(mktime(decoded_aspen_time.timetuple())) * 1000
    return js_time


def py_to_aspen_dt(py_date):
    if isinstance(py_date, str):
        py_date = parser.parse(py_date)
    return py_date.strftime('%d-%b-%y %H:%M:%S').upper()


def parse_aspen_utc(utc_str):
    return utc_str.strptime(utc_str, '%Y-%m-%d %H:%M:%S')


def parse_xml(root):
    all_records = []
    for i, child in enumerate(root):
        record = {}
        for sub_child in child:
            record[sub_child.tag] = sub_child.text
        all_records.append(record)
    return all_records


def anonymize_tag_name(tag, secret_key):
    return hashlib.sha512(tag.encode('utf-8') + secret_key.encode('utf-8')).hexdigest()[:8]