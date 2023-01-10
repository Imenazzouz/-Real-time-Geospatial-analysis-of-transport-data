import yaml
import json
import requests
import kafka as kf
import time
producer = kf.KafkaProducer(bootstrap_servers=['ubuntu:9092'], api_version=(0, 10,1))
#Check api-key
url = 'https://api-v3.mbta.com/vehicles'
headers = {'accept': 'text/event-stream', 'x-api-key': '2c7bed769c2649f6a0dca25c6b177ea0'}
r = requests.get(url, headers=headers, stream=True)
ev = ""
total_bytes = 0
print("Starting ...")
for line in r.iter_lines():
    # filter out keep-alive new lines
    if line:
        decoded_line = line.decode('utf-8')
        total_bytes = total_bytes + len(decoded_line)
        if decoded_line.startswith('event:'):
            ev = yaml.load(decoded_line, Loader=yaml.FullLoader)
            print('fine here')
        else:
            try:
                d = {'event':ev['event'], 'data':yaml.load(decoded_line, Loader=yaml.FullLoader)['data']}
                print(d)
                print("Bytes Received:", total_bytes, end='\r')
                producer.send("mbta-msgs", key=bytes(str(int(time.time())), encoding='utf-8'),
                            value=bytes(json.dumps(d), encoding='utf-8'))
                producer.flush()
                print('sent to mbta-msgs broker')
            except:
                print("Error")