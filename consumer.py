from confluent_kafka import Consumer, KafkaException
from config import config

def set_consumer_configs():
    config['group.id'] = 'AIS_DATA'
    config['auto.offset.reset'] = 'earliest'
    config['enable.auto.commit'] = False

if __name__ == '__main__':
    set_consumer_configs()
    consumer = Consumer(config)
    consumer.subscribe(['pksqlc-qqdj2AIS_FILTERED_2'])

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
                break  # Stop polling after first message is received
    except KeyboardInterrupt:
        print('Canceled by user.')
    finally:
        consumer.close()

    # Store the message value in a variable
    message_value = val
    print(f'Stored message value: {message_value}')