# -*- coding: utf-8 -*-
# -----------------------------------------------------
import pigpio
import time
from datetime import datetime
import sys
import dateutil.relativedelta
import os
import sqlite3
import signal
import logging
from useful import *

logging.basicConfig(filename='sensor_water_db_log.log',level=logging.DEBUG)
logging.info('----------------------------------------------------------------------------')
logging.info('Starting the script in UTC: '+datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))

pid = os.getpid()
# -----------------------------------------------------
# Pino do sinal do sensor
FLOW_SENSOR = 14
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
# Conexao no banco
try:
	db = sqlite3.connect("WATER_SENSOR.db")
	cursorDB = db.cursor()
except Exception as e:
	logging.error('Error trying to connect to database')
	sqliteErrorHandler(e,db,pi,logging)
logging.info("Connected to SQLite3 database")
# ------------------------------------------------------
# Verifica a ultima leitura. se a tabela está vazia ou se a leitura ainda nao acabou
try:
	cursorDB.execute ("SELECT * FROM read_historical WHERE 1 ORDER BY id DESC LIMIT 1")
	reading = cursorDB.fetchone()
	print reading
except Exception as e:
	logging.error("Error trying to connect to acces the last line at historical")
	sqliteErrorHandler(e,db,pi,logging)
logging.info("Last read get from SQLite database: {} ".format(reading))
# ------------------------------------------------------
# Situacao onde nao existe nada no historico
if reading is None:
	total_liters = 0
	try:
		timestamp = int(time.time())
		cursorDB.execute("INSERT INTO read_historical (unix_start,start_date,liters,pid) values ({},datetime(),0,{})".format(timestamp,pid))	
		db.commit()
		cursorDB.execute("SELECT id FROM read_historical WHERE 1 ORDER BY id DESC LIMIT 1")
		reading = cursorDB.fetchone()
		atual_read_id = reading[0]
	except Exception as e:
		logging.error("Error trying to insert the first row at historical table.")
		sqliteErrorHandler(e,db,pi,logging)
	logging.info("First read at historical insert, id: {}".format(atual_read_id))
# ------------------------------------------------------
# Situacao onde existe uma leitura inacabada
elif reading is not None and reading[2] is None:
	atual_read_id = reading[0]
	try:
		cursorDB.execute("SELECT liters FROM sensor_data WHERE 1 ORDER BY unix DESC LIMIT 1")
		reading = cursorDB.fetchone()
		cursorDB.execute("UPDATE read_historical SET pid={} WHERE id={} ".format(pid,atual_read_id));	
		db.commit()
		if reading is not None:
			total_liters = reading[0]
		else: total_liters = 0
	except Exception as e:
		logging.error("Error trying to read last amount of liters read")
		sqliteErrorHandler(e,db,pi,logging)
	logging.info("Not finished read at historical, continue reading, read id: {}".format(atual_read_id))
# ------------------------------------------------------
# Situacao onde existe uma leitura finalizada
elif reading[2] is not None and reading[5] is not None:
	total_liters = 0 
	try:
		timestamp = int(time.time())
		cursorDB.execute("INSERT INTO read_historical (unix_start,start_date,liters,pid) values ({},datetime(),0,{})".format(timestamp,pid))	
		db.commit()
		cursorDB.execute("SELECT id FROM read_historical WHERE 1 ORDER BY id DESC LIMIT 1")
		reading = cursorDB.fetchone()
		atual_read_id = reading[0]
	except Exception as e:
		logging.error("Error trying to insert the next read row at historical table.")
		sqliteErrorHandler(e,db,pi,logging)
	logging.info("New read started!, pid: {}".format(atual_read_id))
# --------------------------------------------------------
#Funcao para poder dar kill e finalizar corretamente o script
stop = False
def handler(number, frame):
	global stop
	stop = True
signal.signal(signal.SIGUSR1, handler)
# --------------------------------------------------------
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
# Condicoes iniciais do sistema e calibracoes
ml_in_pulse = float(1000)/391
count = 0
start_time = time.time()
# --------------------------------------------------------
logging.info("Starting the loop...")
try:
	while True:
		# Pulsos por litro: 420
		# Frequência (Hz) = 7,5*Fluxo(L/min)
		# Fluxo = Freq(Hz)/7,5
		tdelta = (time.time()-start_time)
		# Ao Contar 1 segundo entre no if e salve as caracteristicas do sinal.
		if tdelta >= 1:
			callback.cancel()
		
			flow = float(float(count)/tdelta)/7.5
			total_liters = ml_in_pulse*count + total_liters

			try:
				timestamp = int(time.time())
				cursorDB.execute("INSERT INTO sensor_data values ({},{},{})".format(timestamp,flow,total_liters));	
				db.commit()
			except Exception as e:
				logging.error("Error trying to insert sensor read data")
				sqliteErrorHandler(e,db,pi,logging)
			
			count = 0
			tdelta = 0
			start_time = time.time()
			callback = pi.callback(FLOW_SENSOR, pigpio.RISING_EDGE, callback_func)

		if stop:
			logging.warn('Breaking the loop!')
			break

except KeyboardInterrupt:
	logging.warn('Breaking the loop!')
# ---------------------------------------------------------
# Salvando no banco o tanto de litros e a data desse script
try:
	timestamp = int(time.time())
	cursorDB.execute("UPDATE read_historical SET unix_end={}, end_date=datetime(), liters={} WHERE id={} ".format(timestamp,total_liters,atual_read_id));	
	db.commit()
except Exception as e:
	logging.error("Error trying to finish the read")
	sqliteErrorHandler(e,db,pi,logging)
# ----------------------------------------------------------
db.rollback()
logging.warn('Database rollback')
db.close()
logging.warn('Database close connection')		
pi.stop()
logging.warn('PIGPIO stop')
# ----------------------------------------------------------
logging.warn('Finishing the script...UTC:'+datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))
