from kafka import KafkaProducer
from gen_http_logs import IPGenerator
from gen_http_logs import LogGenerator
from argparse import ArgumentParser
from atexit import register
from Logger import logger
import _thread


def createKafkaProducer(bs):
    producer = KafkaProducer(bootstrap_servers= bs)
    return producer

def shutdown_thread(prod):
    _thread.start_new_thread(shutdown_hook,(prod,))

def shutdown_hook(producer):
    logger.info('stopping application...')
    logger.info('shutting down http log generator...')
    logger.info('closing producer...')
    producer.close()
    logger.info('done!')

def run(bootstrap_servers,topic):
    ipgen = IPGenerator(100, 10)
    producer = createKafkaProducer(bootstrap_servers)
    register(shutdown_thread,prod=producer)
    logger.info('Connecting to Kafka...')
    try:
        LogGenerator(ipgen).write_qps(producer, topic,0.05)
    finally:
        final()
        logger.info('End of Application')
    
   

def main(bootstrap_servers='localhost:9092',topic='test'):
    run(bootstrap_servers,topic)

if __name__=='__main__':
    parser = ArgumentParser()
    parser.add_argument("-b","--bootstrap-servers",help="sets bootstrap_servers and port, default localhost:9092")
    parser.add_argument("-t","--topic",help="sets kafka topic, default will go to test topic ")
    args = parser.parse_args()
    main(args.bootstrap_servers, args.topic)



