import logging



logger = logging.getLogger('gen_iot_logs')
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
fh = logging.FileHandler('gen_iot_logs.log')
fh.setFormatter(formatter)
fh.setLevel(logging.DEBUG)
logger.addHandler(ch)
logger.addHandler(fh)