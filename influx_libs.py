"""

InfluxDb helper classes, which take configuraton from config file, so you don't
have to hard code passwords in code. By default it's in ./etc/influxdb.ini

"""

import configparser
from influxdb import InfluxDBClient

influx_config_file = "./etc/influxdb.ini"

class InfluxLibs: 

    def __init__(self, config_file=influx_config_file, dbname=None):
        self.config_file = config_file

        self.read_config()
        self.set_parameters()
        

    def read_config(self):
        self.config = configparser.ConfigParser()
        self.config.read(self.config_file)

    def set_parameters(self):

        self.host = self.config['influx']['host']
        self.port = self.config['influx']['port'] 
        self.user = self.config['influx']['user']
        self.password = self.config['influx']['password']
        self.dbname = self.config['influx']['dbname'] 

    def get_client_connection(self):
    
        self.client = InfluxDBClient(self.host, self.port, self.user, self.password, self.dbname)
        return self.client

    def format_infuxdb(self, measurement_name, json, tags):

        """
        Takes measurement_name, json data as dictionary and tags, it returns influxDB formatted json_body
        ready to call write to DB.
        """

        json_body = [
            { 
                "time": json.pop('timestamp'),
                "measurement": measurement_name,
                "tags": {},
                "fields": {}
            }]

        for tag in tags: 
            json_body[0]['tags'][tag] = json.pop(tag)

        for key in json:
            json_body[0]['fields'][key] = json[key]

        print(json_body)
        return json_body


    def write(self, json_body):
        print(f"DEBUG: WebleyInflux.write: json_body={json_body}")
        return self.client.write_points(json_body)


if __name__ == "__main__":

    influx_libs = InfluxLibs()
    influx_client = influx_libs.get_client_connection()

    data_dict = {'timestamp': '2019-05-02T11:29:22.2422', 'account': '111111', 'http': 'POST', 'url': 'mobile_settings', 'response_time': 0.04998}
    influx_libs.format_infuxdb('mobile_api', data_dict, tags=['http', 'url'])


