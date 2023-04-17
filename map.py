import pygame
import random
import ast
from confluent_kafka import Consumer, KafkaException
from config import config
#import fileinput

# on my windows machine $ echo [500, 100] | python3 \\wsl.localhost\Ubuntu\home\jackfmurphy\confluent_files\ais-demo\kafka_ais_project\map.py 

# on mac $ echo "[500,100]" | python3 map.py

def convert_geographic_coordinate_to_pixel_value(lon, lat, transform):
    """
    Converts a latitude/longitude coordinate to a pixel coordinate given the
    geotransform of the image.
    Args:
        lon: Pixel longitude.
        lat: Pixel latitude.
        transform: The geotransform array of the image.
    Returns:
        Tuple of refx, refy pixel coordinates.
    """

    xOrigin = transform[0]
    yOrigin = transform[3]
    pixelWidth = transform[1]
    pixelHeight = -transform[5]

    refx = round((lon - xOrigin) / pixelWidth)
    refy = round((yOrigin - lat) / pixelHeight)

    return refx, refy


def set_consumer_configs():
    config['group.id'] = 'AIS_DATA'
    config['auto.offset.reset'] = 'earliest'
    config['enable.auto.commit'] = False



def topic_consumer():

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
    
    print(f'Stored message value: {val}')

    return val


    # Store the message value in a variable


if __name__ == "__main__":

    #variables
    transform = [0.0, 5, 0.0, 0, 0.0, 5]
    alt = 0
    origin = 0
    running = True
    draw = [] #At a later stage this will need to be a dictionary

    #data from the topic
    boat = topic_consumer()
    for i in boat:
        print(i) 
    lat, lon = boat["LAT"], boat["LON"]  


    pixlon,pixlat = convert_geographic_coordinate_to_pixel_value(lon, lat, transform)

    #In the future this will grab the next line of standard input and will be used for adding a new boat to the graph.
    # def next_boat():
    #     return random.randint(0, 895), random.randint(0,595)

    #temp testing the input
    def next_boat():
         return lon, lat


    pygame.init()

    pygame.display.set_caption('Kafka AIS Demo')
    screen = pygame.display.set_mode([901,601])
    screen.fill([0,0,0])

    while running:

        #lon and lat will need to be populated by the corresponding values from the standard input

        #this does not currently work for more than one datum

        draw.append([pixlon,pixlat])

        #this could be tidied
        for lon_lat_tuple in draw:
            pygame.draw.circle(screen, (0, 255, 255), (pixlon,pixlat), 2,0)
            #https://www.pygame.org/docs/ref/draw.html#pygame.draw.circle
         
         
        pygame.display.update()

        pixlon, pixlat = next_boat()
        
        #Optional timer to slow it down
        pygame.time.wait(1000)

        # for line in fileinput.input():
        #     print(line)
        for event in pygame.event.get():
            if event.type == pygame.KEYDOWN:
                if event.key == pygame.K_SPACE: 
                    running = False