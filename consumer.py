from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka import Consumer, KafkaException
from config import config

<<<<<<< Updated upstream
def set_consumer_configs():
    config['group.id'] = 'hello_group'
    config['auto.offset.reset'] = 'earliest'
    config['enable.auto.commit'] = False
    
def assignment_callback(consumer, partitions):
    for p in partitions:
        print(f'Assigned to {p.topic}, partition {p.partition}')

=======
schema_str = """{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "MMSI": {
      "type": "number"
    },
    "LAT": {
      "type": "number"
    },
    "LON": {
      "type": "number"
    }
  },
  "required": ["MMSI", "LAT", "LON"]
}"""

class Vessel(object):
    def __init__(self, mmsi, lat, lon):
        self.mmsi = mmsi
        self.lat = lat
        self.lon = lon

def dict_to_vessel(dict, ctx):
    return Vessel(dict['MMSI'], dict['LAT'], dict['LON'])
>>>>>>> Stashed changes

if __name__ == '__main__':
    topic = 'vessel_positions'

    deserializer = JSONDeserializer(schema_str, from_dict=dict_to_vessel)

    config['group.id'] = 'vessel_group'
    config['auto.offset.reset'] = 'earliest'

    consumer = Consumer(config)
    consumer.subscribe(['hello_topic'], on_assign=assignment_callback)    
    try:
<<<<<<< Updated upstream
        while True:
            event = consumer.poll(1.0)
            if event is None:
                continue
            if event.error():
                raise KafkaException(event.error())
            else:
                val = event.value().decode('utf8')
                partition = event.partition()
                print(f'Received: {val} from partition {partition}    ')
                consumer.commit(event)        
=======
        event = consumer.poll(1.0)
        if event is not None:
            message = json_deserializer(event.value(),
                                       SerializationContext(topic, MessageField.VALUE))
            
            vessel = deserializer(message)

            if vessel is not None:
                print(f'Vessel with MMSI {vessel.mmsi} is at {vessel.lat} lat, {vessel.lon} lon.')

                # Consume just one message and commit the offset
            while True:
                event = consumer.poll(1.0)
                if event is None:
                    continue
                if event.error():
                    raise KafkaException(event.error())
                else:
                    val = event.value().decode('utf8')
                    partition = event.partition()
                    print(f'Received: {val} from partition {partition}    ')
                    consumer.commit(event)
                    break  # Stop polling after first message is received
>>>>>>> Stashed changes
    except KeyboardInterrupt:
        pass

<<<<<<< Updated upstream
    while True:
        event = consumer.poll(1.0)
        if event is None:
            continue
        if event.error():
            raise KafkaException(event.error())
        else:
            val = event.value().decode('utf8')
            partition = event.partition()
            print(f'Received: {val} from partition {partition}    ')
            # consumer.commit(event)
=======
    consumer.close()
>>>>>>> Stashed changes
