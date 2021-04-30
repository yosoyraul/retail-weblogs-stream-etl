from kafka import KafkaProducer
from gen_http_logs import IPGenerator
from gen_http_logs import LogGenerator
from atexit import register
from Logger import logger
import _thread


def createKafkaProducer():
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    return producer

def shutdown_thread(prod):
    _thread.start_new_thread(shutdown_hook,(prod,))

def shutdown_hook(producer):
    logger.info('stopping application...')
    logger.info('shutting down http log generator...')
    logger.info('closing producer...')
    producer.close()
    logger.info('done!')

def run():
    ipgen = IPGenerator(100, 10)
    producer = createKafkaProducer()
    register(shutdown_thread,prod=producer)
    logger.info('Connecting to Kafka...')
    try:
        LogGenerator(ipgen).write_qps(producer, 1)
    finally:
        final()
        logger.info('End of Application')
    
   

def main():
    run()

if __name__=='__main__':
    main()




