from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka import Consumer, KafkaException
from config import config

def set_consumer_configs():
    config['group.id'] = 'hello_group'
    config['auto.offset.reset'] = 'earliest'
    config['enable.auto.commit'] = False
    
def assignment_callback(consumer, partitions):
    for p in partitions:
        print(f'Assigned to {p.topic}, partition {p.partition}')


if __name__ == '__main__':
    topic = 'vessel_positions'

    deserializer = JSONDeserializer(schema_str, from_dict=dict_to_vessel)

    config['group.id'] = 'vessel_group'
    config['auto.offset.reset'] = 'earliest'

    consumer = Consumer(config)
    consumer.subscribe(['hello_topic'], on_assign=assignment_callback)    
    try:
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
    except KeyboardInterrupt:
        pass

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
