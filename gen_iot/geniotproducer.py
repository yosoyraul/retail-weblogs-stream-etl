from kafka import KafkaProducer
from iot_devices import get_profile,gen_iot
from atexit import register
from logger import logger
from sys import argv
import _thread


def createKafkaProducer():
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    return producer

def shutdown_thread(prod):
    _thread.start_new_thread(shutdown_hook,(prod,))

def shutdown_hook(producer):
    logger.info('stopping application...')
    logger.info('shutting down iot generator...')
    logger.info('closing producer...')
    producer.close()
    logger.info('done!')


def run():
    profile_name, profile = get_profile(argv)
    producer = createKafkaProducer()
    register(shutdown_thread,prod=producer)
    logger.info('Connecting to Kafka...')
    gen_iot(profile_name,profile,producer)
    
   

def main():
    run()

if __name__=='__main__':
    main()




