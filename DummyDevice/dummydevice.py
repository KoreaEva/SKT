import random
import time
import sys
import iothub_client
from iothub_client import *
from iothub_client_args import *

timeout = 241000
minimum_polling_time = 9

# messageTimeout - the maximum time in milliseconds until a message times out.
# The timeout period starts at IoTHubClient.send_event_async. 
# By default, messages do not expire.
message_timeout = 10000

receive_context = 0
received_count = 0

# global counters
receive_callbacks = 0
send_callbacks = 0

# chose HTTP, AMQP or MQTT as transport protocol
protocol = IoTHubTransportProvider.AMQP

# String containing Hostname, Device Id & Device Key in the format:
# "HostName=<host_name>;DeviceId=<device_id>;SharedAccessKey=<device_key>"
#connection_string = "HostName=winkeyiot.azure-devices.net;DeviceId=Device1;SharedAccessKeyName=device;SharedAccessKey=TimJbgVT1XtFm1h+5Ljym5yELlSSSS/xnPYqoxUs2wk="
connection_string = "HostName=SKTUdpHub.azure-devices.net;DeviceId=device1;SharedAccessKeyName=iothubowner;SharedAccessKey=ZVb0YBz1pw5KlcB8yZng04zndDuTie4NcZScddpLo8Y="
msg_txt = "{\"DeviceID\":\"Device1\",\"Temperature\":%d,\"Humidity\":%d,\"Dust\":%d}"

if __name__ == '__main__':
    print("\nPython %s" % sys.version)
    print("IoT Hub for Python SDK Version: %s" % iothub_client.__version__)

    try:
        (connection_string, protocol) = get_iothub_opt(sys.argv[1:], connection_string, protocol)
    except OptionError as o:
        print(o)
        usage()
        sys.exit(1)

    print("Starting the IoT Hub Python sample...")
    print("    Protocol %s" % protocol)
    print("    Connection string=%s" % connection_string)

    iothub_client_sample_run()

def usage():
    print("Usage: iothub_client_sample.py -p <protocol> -c <connectionstring>")
    print("    protocol        : <amqp, http, mqtt>")
    print("    connectionstring: <HostName=<host_name>;DeviceId=<device_id>;SharedAccessKey=<device_key>>")


def iothub_client_sample_run():

    try:

        iotHubClient = iothub_client_init()                                             #iotHub_client_init()

        i = 1
        print("IoTHubClient waiting for commands, press Ctrl-C to exit")

        while True:
            ####################################################                    수정된 부분
            temperature = random.randrange(25, 32)
            humidity = random.randrange(60, 80)
            dust = 50 + temperature + random.randrange(1,5)
            
            msg_txt_formatted = msg_txt % (temperature, humidity, dust)
            ####################################################

            message = IoTHubMessage(msg_txt_formatted)
            # optional: assign ids
            message.message_id = "message_%d" % i
            message.correlation_id = "correlation_%d" % i
            # optional: assign properties
            prop_map = message.properties()
            prop_text = "PropMsg_%d" % i
            prop_map.add("Property", prop_text)
            iotHubClient.send_event_async(message, send_confirmation_callback, i)

            print("SEND MESSAGE:" + msg_txt_formatted)

            time.sleep(5)
            i += 1

    except IoTHubError as e:
        print("Unexpected error %s from IoTHub" % e)
        return
    except KeyboardInterrupt:
        print("IoTHubClient sample stopped")

    print_last_message_time(iotHubClient)

def iothub_client_init():
    # prepare iothub client
    iotHubClient = IoTHubClient(connection_string, protocol)
    if iotHubClient.protocol == IoTHubTransportProvider.HTTP:
        iotHubClient.set_option("timeout", timeout)
        iotHubClient.set_option("MinimumPollingTime", minimum_polling_time)
    # set the time until a message times out
    iotHubClient.set_option("messageTimeout", message_timeout)
    # some embedded platforms need certificate information
    # set_certificates(iotHubClient)
    # to enable MQTT logging set to 1
    if iotHubClient.protocol == IoTHubTransportProvider.MQTT:
        iotHubClient.set_option("logtrace", 0)
    iotHubClient.set_message_callback(
        receive_message_callback, receive_context)
    return iotHubClient

def receive_message_callback(message, counter):
    global receive_callbacks
    buffer = message.get_bytearray()
    size = len(buffer)
    print("###################################################")
    print("    Data: <<<   %s   >>> & Size=%d" % (buffer[:size].decode('utf-8'), size))
    print("###################################################")

    map_properties = message.properties()
    key_value_pair = map_properties.get_internals()
    #print("    Properties: %s" % key_value_pair)
    counter += 1
    receive_callbacks += 1
    #print("    Total calls received: %d" % receive_callbacks)
    return IoTHubMessageDispositionResult.ACCEPTED

def send_confirmation_callback(message, result, user_context):
    global send_callbacks
    print(
        "Confirmation[%d] received for message with result = %s" %
        (user_context, result))
    map_properties = message.properties()
    #print("    message_id: %s" % message.message_id)
    #print("    correlation_id: %s" % message.correlation_id)
    key_value_pair = map_properties.get_internals()
    #print("    Properties: %s" % key_value_pair)
    send_callbacks += 1
    #print("    Total calls confirmed: %d" % send_callbacks)