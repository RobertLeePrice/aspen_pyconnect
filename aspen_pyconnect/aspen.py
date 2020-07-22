from requests import Session
from requests.auth import HTTPBasicAuth
from zeep import Client
from zeep.transports import Transport
import xml.etree.ElementTree as ET
from dateutil import relativedelta
from datetime import datetime, timedelta, timezone
from math import ceil


class IP21Connector(object):
    def __init__(self, server, user, pw):
        self.user = user
        self.pw = pw
        self.server = server
        self.client = ''

    def connect(self):
        session = Session()
        session.auth = HTTPBasicAuth(self.user, self.pw)
        url = 'http://' + self.server + '/SQLPlusWebService/SQLPlusWebService.asmx'
        self.client = Client(url + '?WSDL', transport=Transport(session=session))
    
    @staticmethod
    def remove_timezone(dt):
        if dt.tzinfo is not None and dt.tzinfo.utcoffset(dt) is not None:
            dt = dt.replace(tzinfo=None)
        return dt
    
    @staticmethod
    def convert_to_aspen_dt(pydt):
        return pydt.strftime('%d-%b-%y %H:%M:%S').upper()
    
    @staticmethod
    def calculate_period_in_seconds(period):
        return (datetime.strptime(period, '%H:%M:%S') - datetime(1900, 1, 1)).total_seconds()

    @staticmethod
    def calculate_points_in_time_range(start_time, end_time, period_in_seconds):
        return ceil((end_time - start_time).total_seconds() / period_in_seconds)
    
    @staticmethod
    def parse_xml(root):
        all_records = []
        for i, child in enumerate(root):
            record = {}
            for sub_child in child:
                record[sub_child.tag] = sub_child.text
            all_records.append(record)
        return all_records

    def history(self, start_time, end_time, tag_name, request=1, period='00:05:00', stepped=0, pull_limit=20000):

        parsed_data = []

        start_time = self.remove_timezone(start_time)
        end_time = self.remove_timezone(end_time)

        period_in_seconds = self.calculate_period_in_seconds(period)
        num_points = self.calculate_points_in_time_range(start_time, end_time, period_in_seconds)

        if num_points >= pull_limit:

            update_end_time = start_time
            update_start_time = start_time

            while update_end_time < end_time:

                update_end_time += relativedelta.relativedelta(seconds=(period_in_seconds * pull_limit))

                sql = """
                        SELECT ISO8601(ts) AS "utc_time", name AS tag_name, value FROM history(80)
                        WHERE name='{tag_name}' AND ts 
                        BETWEEN '{start_time}' AND '{end_time}' AND request={request} AND period={period} 
                        AND stepped={stepped}
                    """.format(
                            tag_name=tag_name,
                            start_time=self.convert_to_aspen_dt(update_start_time),
                            end_time=self.convert_to_aspen_dt(update_end_time),
                            period=period,
                            request=request,
                            stepped=stepped)

                result = self.client.service.ExecuteSQL(sql)

                for data_point in self.parse_xml(ET.fromstring(result)):
                    parsed_data.append(data_point)

                update_start_time = update_end_time

        else:

            sql = """
                    SELECT ISO8601(ts) AS "utc_time", name AS tag_name, value FROM history(80)
                    WHERE name='{tag_name}' AND ts 
                    BETWEEN '{start_time}' AND '{end_time}'  AND request={request} AND period={period}
                """.format(
                tag_name=tag_name,
                start_time=self.convert_to_aspen_dt(start_time),
                end_time=self.convert_to_aspen_dt(end_time),
                period=period,
                request=request)

            result = self.client.service.ExecuteSQL(sql)

            parsed_data = self.parse_xml(ET.fromstring(result))

        return parsed_data

    def get_value(self, name, timestamp=datetime.now()):
        timestamp = self.convert_to_aspen_dt(timestamp)

        sql = """SELECT ISO8601(ts) AS "utc_time", value FROM history WHERE name='{name}' 
            AND ts='{timestamp}' AND request=2""".format(name=name, timestamp=timestamp)

        result = self.client.service.ExecuteSQL(sql)
        root = ET.fromstring(result)
        return self.parse_xml(root)

    def get_last_actual_value(self, name):

        sql = """SELECT ISO8601(ts) AS "utc_time", value FROM history WHERE name='{name}'
            AND ts > (SELECT ISO8601(ts, 0) FROM history WHERE name='{name}' AND request=4)
            AND request=4""".format(name=name)

        result = self.client.service.ExecuteSQL(sql)
        root = ET.fromstring(result)
        return self.parse_xml(root)

    def get_all_tag_definitions(self):
        sql = """SELECT
                    'IP_TextDef' AS source_table,
                    NAME AS tag_name,   
                    IP_DESCRIPTION AS tag_description, 
                      IP_PLANT_AREA AS plant_area, 
                    IP_TAG_TYPE AS tag_type,
                      '' AS eng_units,
                      IP_VALUE AS example_value,
                      '' AS stepped    
                FROM IP_TextDef
                UNION
                SELECT
                    'IP_AnalogDef' AS source_table,
                    NAME AS tag_name,   
                    IP_DESCRIPTION AS tag_description, 
                      IP_PLANT_AREA AS plant_area, 
                    IP_TAG_TYPE AS tag_type,
                      IP_ENG_UNITS AS eng_units,
                      IP_VALUE AS example_value,
                      IP_STEPPED AS stepped    
                FROM IP_AnalogDef
                UNION
                SELECT
                    'IP_DiscreteDef' AS source_table,
                    NAME AS tag_name,   
                    IP_DESCRIPTION AS tag_description, 
                      IP_PLANT_AREA AS plant_area, 
                    IP_TAG_TYPE AS tag_type,
                      IP_ENG_UNITS AS eng_units,
                      IP_VALUE AS example_value,
                      IP_STEPPED AS stepped    
                FROM IP_DiscreteDef"""

        result = self.client.service.ExecuteSQL(sql)
        root = ET.fromstring(result)
        return self.parse_xml(root)

    def tags_list(self, tags):

        tags_str = ""

        for i in range(0, len(tags) - 1):
            tags_str += "'" + tags[i] + "', "

        tags_str += "'" + tags[-1] + "'"

        sql = """SELECT
                    NAME AS tag_name,   
                    IP_DESCRIPTION AS tag_description, 
                      IP_PLANT_AREA AS plant_area, 
                    IP_TAG_TYPE AS tag_type,
                      '' AS eng_units,
                      IP_VALUE AS example_value,
                      '' AS stepped    
                FROM IP_TextDef WHERE NAME IN ({tags})
                UNION
                SELECT
                    NAME AS tag_name,   
                    IP_DESCRIPTION AS tag_description, 
                      IP_PLANT_AREA AS plant_area, 
                    IP_TAG_TYPE AS tag_type,
                      IP_ENG_UNITS AS eng_units,
                      IP_VALUE AS example_value,
                      IP_STEPPED AS stepped    
                FROM IP_AnalogDef WHERE NAME IN ({tags})
                UNION
                SELECT
                    NAME AS tag_name,   
                    IP_DESCRIPTION AS tag_description, 
                      IP_PLANT_AREA AS plant_area, 
                    IP_TAG_TYPE AS tag_type,
                      IP_ENG_UNITS AS eng_units,
                      IP_VALUE AS example_value,
                      IP_STEPPED AS stepped    
                FROM IP_DiscreteDef WHERE NAME IN ({tags})""".format(tags=tags_str)

        result = self.client.service.ExecuteSQL(sql)
        root = ET.fromstring(result)
        return self.parse_xml(root)