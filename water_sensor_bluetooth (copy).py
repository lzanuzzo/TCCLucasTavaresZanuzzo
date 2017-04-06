# -*- coding: utf-8 -*-
# -----------------------------------------------------
import time
import sys
import os
from datetime import datetime
import bluetooth
from bluetooth import *
import MySQLdb
from useful import *
import signal
import logging


logging.basicConfig(filename='water_sensor_bluetooth_log.log',level=logging.DEBUG)
logging.info('----------------------------------------------------------------------------')
logging.info('Starting the script in UTC: '+datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))
# ------------------------------------------------------
# Conexao bluetooth
server_sock,port = bluetoothConnection() 
#port[1] = avaible port
#port[0] = device
logging.info("Waiting for connection on RFCOMM channel %d" % port[1])
try:
	client_sock, client_info = server_sock.accept() #Server accepts connection request from a client; client_socket is the socket used for communication with client and address the client's address.
except KeyboardInterrupt:
	logging.info("Interrupting script during bluetooth connection acceptance")
	logging.warn('Finishing the script...UTC:'+datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))
	stop_advertising(server_sock)
	exit()
client_sock.settimeout(0.9) #Timeout waiting for a client data
logging.info("Accepted connection from {}".format(client_info))
# ------------------------------------------------------
# Conexao no banco
try:
	db = MySQLdb.connect("localhost", "wiki", "eeyrfxfe", "WATER_SENSOR")
	cursorDB = db.cursor()
except Exception as e:
	logging.error('Error trying to connect to database')
	mysqlErrorHandler_bt(e,db,logging)
logging.info("Connected to MySQL database")
# --------------------------------------------------------
#Funcao para poder dar kill e finalizar corretamente o script
stop = False
def handler(number, frame):
	global stop
	stop = True
signal.signal(signal.SIGUSR1, handler)
# --------------------------------------------------------
logging.info("Starting the loop...")
try:
    while True:
    	# the loop countdown stay in bluetooth acepptance rate
		try:
			data_received = client_sock.recv(1024)
			if data_received == 'k':
				logging.info('Command to kill the read process')
				# Verifica a ultima leitura. se a tabela está vazia ou se a leitura ainda nao acabou
				try:
					cursorDB.execute ("SELECT pid FROM read_historical WHERE unix_end IS NULL ORDER BY unix_start DESC LIMIT 1")
					reading = cursorDB.fetchone()
					logging.info("Last read get from MySQL database: {} ".format(reading))
					if reading is None:
						logging.info("There is no read to finish...")
					else:
						os.system("sudo kill -10 {}".format(reading[0]))
						logging.info('Killing the process with pid: {}'.format(reading[0]))
				except Exception as e:
					logging.info("Error trying to connect to access the last line at historical")
					mysqlErrorHandler_bt(e,db,logging)
			elif data_received == 'e':
				logging.info("Command to begin a read")
				try:
					cursorDB.execute ("SELECT pid FROM read_historical WHERE unix_end IS NULL ORDER BY unix_start DESC LIMIT 1")
					reading = cursorDB.fetchone()
					logging.info("Last read get from MySQL database: {} ".format(reading))
					if reading is None:
						os.system("sudo nice -20 python sensor_water_db.py > log_exec.txt &")
						logging.info("Starting a new process to read")
				except Exception as e:
					logging.info("Error trying to connect to access the last line at historical")
					mysqlErrorHandler_bt(e,db,logging)
			elif data_received == 'h':
				logging.info("Command to read historic")
				try:
					cursorDB.execute ("SELECT * FROM read_historical")
					reading = cursorDB.fetchall()
					logging.info("Get all historical from MySQL database")

					historical_read = True
					chart_and_delete = True

					while historical_read:
						try:
							client_sock.send("initialhistoric\n")
							for row in reading:					
									client_sock.send("{},{},{},{},{},{}\n".format(row[0],row[1],row[2],row[3],row[4],row[5]))
							client_sock.send("finalhistoric\n")
						except bluetooth.btcommon.BluetoothError as error:
							logging.warn("Could not connect: {}; Retrying...".format(error))
							#server_sock, port = bluetoothConnection()
							client_sock, client_info = server_sock.accept()
							client_sock.settimeout(0.9)
							logging.info("Accepted connection from {}".format(client_info))

						try:
							data_received = client_sock.recv(1024)	
							if data_received == 'h':
								historical_read = False
								logging.info("Command to finish the read historic")
						except Exception as e:
							x = 1		

					"""while chart_and_delete:
						try:
							data_received = client_sock.recv(1024)
							char_func, id_read = data_received.split(":")
							if char_func == 'd':
								logging.info("Command to begin a read")
								try:
									cursorDB.execute ("SELECT pid FROM read_historical WHERE unix_end IS NULL ORDER BY unix_start DESC LIMIT 1")
									reading = cursorDB.fetchone()
									logging.info("Last read get from MySQL database: {} ".format(reading))
									if reading is None:
										os.system("sudo nice -20 python sensor_water_db.py > log_exec.txt &")
										logging.info("Starting a new process to read")
								except Exception as e:
									logging.info("Error trying to connect to access the last line at historical")
									mysqlErrorHandler_bt(e,db,logging)
							elif char_func == 'c':

							elif char_func == 'h':
								chart_and_delete = False
								logging.info("Command to finish the chart or delete wait")

						except Exception as e:
							x = 1"""

				except Exception as e:
					logging.info("Error trying to connect to access the historical")
					mysqlErrorHandler_bt(e,db,logging)


		except bluetooth.btcommon.BluetoothError as error:
			#logging.error("Bluetooth Error: {}".format(error))
			# Timeout catcher
			x = 1

		try:
			cursorDB.execute ("SELECT * FROM sensor_data ORDER BY unix DESC LIMIT 1")
			reading = cursorDB.fetchone()
			db.commit()
			
			data_to_send = "{};{};{}\r\n".format(reading[0],reading[1],reading[2])

		except Exception as e:
			logging.info("Error trying to read the DB, to send on time data")
			logging.error("MySQL Error: {}".format(str(e)))
			logging.warn("Database rollback")
			db.rollback()

		try:
			client_sock.send(data_to_send)
		
		except bluetooth.btcommon.BluetoothError as error:
			logging.warn("Could not connect: {}; Retrying...".format(error))
			#server_sock, port = bluetoothConnection()
			client_sock, client_info = server_sock.accept()
			client_sock.settimeout(0.9)
			logging.info("Accepted connection from {}".format(client_info))
			
		if stop:
			logging.warn('Breaking the loop!')
			break


except KeyboardInterrupt:
	logging.warn('Breaking the loop!')
	pass
# ------------------------------------------------------
logging.warn("Database rollback")
db.rollback()
logging.warn('Database close connection')	
db.close()	
logging.warn('Bluetooth socket stop advertising')	
stop_advertising(server_sock)
logging.warn('Bluetooth client socket close')	
client_sock.close()
logging.warn('Bluetooth server socket close')	
server_sock.close()
logging.warn('Finishing the script...UTC:'+datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))
# -----------------------------------------------------

