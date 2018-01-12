import json
import logging
import time
import boto3
import paho.mqtt.client as mqtt


class MQTTtoFH():
    """
        Create Firehose and MQTT Clients and upon receiving MQTT
        messages send json objects to Firehose to be stores in the S3 bucket
    """
    def __init__(self):
        self.FirehoseClient = boto3.client('XXXX', aws_access_key_id='XXXX',
                                   aws_secret_access_key='XXXX',
                                   region_name='XXXX')
        self.MQTTClient = mqtt.Client()

    def write_to_firehose(self, reading, firehose_client):
        """
        :param reading: the json message to be sent to Firehose
        :param firehose_client: the Firehose client instant
        :return:
        """
        try:
            response = firehose_client.put_record(
                DeliveryStreamName='XXXX',
                Record={
                    'Data': json.dumps(reading)
                }
            )
        except Exception:
            logging.exception("Problem pushing to firehose")

    def on_connect(self, client, userdata, flags, rc):
        '''
        Subscribe to topics upon connecting to an MQTT broker
        '''
        print("Connected with result code " + str(rc))
        client.subscribe("test/#")

    def on_message(self, client, userdata, msg):
        '''
        Upon receiving an MQTT message, parse it and send a json object to firehose using the write_to_firehose method.
        '''
        variable = msg.topic.rsplit('/', 1)[1]
        topic_before_variable = msg.topic.rsplit('/', 1)[0]
        sensor_name = topic_before_variable.rsplit('/', 1)[1]
        topic_before_sensor = topic_before_variable.rsplit('/', 1)[0]
        station = topic_before_sensor.rsplit('/', 1)[1]
        topic_before_station = topic_before_sensor.rsplit('/', 1)[0]
        line = topic_before_station.rsplit('/', 1)[1]
        plant = topic_before_station.rsplit('/', 1)[0]
        value = {"plant": plant, "line": line, "station": station,
                 "sensor_name": sensor_name,
                 "variable": variable, "value": float(msg.payload)}
        self.write_to_firehose(value, self.FirehoseClient)
        print(msg.topic + " " + str(msg.payload))
        time.sleep(10)

    def mqtt_loop(self):
        '''
        stay in a loop to receive mqtt messages
        '''
        self.MQTTClient.on_connect = self.on_connect
        self.MQTTClient.on_message = self.on_message
        self.MQTTClient.connect("Broker_hostname", 1883, 60)
        self.MQTTClient.loop_forever()


def main():
    logging.basicConfig(level=logging.DEBUG)
    my_insert_ = MQTTtoFH()
    my_insert_.mqtt_loop()

if __name__ == '__main__':
    main()
