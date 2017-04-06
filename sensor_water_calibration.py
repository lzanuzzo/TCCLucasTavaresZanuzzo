# -*- coding: utf-8 -*-
# -----------------------------------------------------
import pigpio
import time
from datetime import datetime
import logging

logging.basicConfig(filename='sensor_water_calibration_log.log',level=logging.DEBUG)
logging.info('----------------------------------------------------------------------------')
logging.info('Starting the script in UTC: '+datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))
# -----------------------------------------------------
# Pino do sinal do sensor
FLOW_SENSOR = 2
logging.info("Flow sensor at PIN: {} ".format(FLOW_SENSOR))
# ------------------------------------------------------
# Config da biblioteca dos GPIO

pi = pigpio.pi()
pigpio.exceptions = True
if not pi.connected:
   logging.error('Error trying to connect to PIGPIO lib')
   exit()
pi.set_mode(FLOW_SENSOR, pigpio.INPUT)  
pi.set_pull_up_down(FLOW_SENSOR, pigpio.PUD_UP)
logging.info('PIGPIO configured')
# ------------------------------------------------------
# Funcao de callback que trigga toda vez que sobe o sinal
def callback_func(gpio, level, tick):
	global count
	count+=1
# --------------------------------------------------------
callback = pi.callback(FLOW_SENSOR, pigpio.RISING_EDGE, callback_func)
# EITHER_EDGE, RISING_EDGE (default), or FALLING_EDGE.
# If a user callback is not specified a default tally callback is provided which simply counts edges.
# The count may be retrieved by calling the tally function. 
# The count may be reset to zero by calling the reset_tally function. 
# --------------------------------------------------------
# Condicoes iniciais
count = 0
# --------------------------------------------------------
try:
	logging.info('Begin loop')
	while True:
		x=1
except KeyboardInterrupt:
	print count
	logging.info("Pulses: {}".format(count))
	logging.warn('Breaking the loop!')

pi.stop()
logging.warn('PIGPIO stop')
# ----------------------------------------------------------
logging.warn('Finishing the script...UTC:'+datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))