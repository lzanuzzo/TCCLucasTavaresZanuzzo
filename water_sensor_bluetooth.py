# -*- coding: utf-8 -*-
# -----------------------------------------------------
import time
import sys
import os
from datetime import datetime
import bluetooth
from bluetooth import *
import sqlite3
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
	db = sqlite3.connect("WATER_SENSOR.db")
	cursorDB = db.cursor()
except Exception as e:
	logging.error('Error trying to connect to database')
	sqliteErrorHandler_bt(e,db,logging)
logging.info("Connected to SQLite3 database")
# --------------------------------------------------------
#Funcao para poder dar kill e finalizar corretamente o script
stop = False
def handler(number, frame):
	global stop
	stop = True
signal.signal(signal.SIGUSR1, handler)
# --------------------------------------------------------
logging.info("Starting the loop...")
# Flags for each type of stream data
historic_loop 	= False;
chart_loop		= False;
read_loop 		= True;
# Countdown to receive data for each type of stream
historic_loop_countdown = 0.05
chart_loop_countdown 	= 0.1
read_loop_countdown 	= 0.9
# Initialize the list with historical and chart data
all_historical_data = []
all_chart_data		= []
count_for_lists 	= 0
# Main loop
# --------------------------------------------------------
try:
    while True:
    	# the loop countdown stay in bluetooth acepptance rate
		try:
			data_received = client_sock.recv(1024)
			if ':' in data_received:
				data_received_with_id, id_read = data_received.split(":")
			else: 
				data_received_with_id = ''
			# --------------------------------------------------------------------------------------------------------------------
			# Commando to retake sensor reader routine
			if data_received == 'r':
				logging.info('Command to begin the read loop')
				logging.info("Setting timeout from socket to {} and setting read loop flag".format(read_loop_countdown))
				count_for_lists = 0
				historic_loop = False
				read_loop = True
				chart_loop = False
				client_sock.settimeout(read_loop_countdown)
			# --------------------------------------------------------------------------------------------------------------------
			# Commando to kill sensor reader script
			elif data_received == 'k':
				logging.info('Command to kill the read process')
				# Verifica a ultima leitura. se a tabela está vazia ou se a leitura ainda nao acabou
				try:
					cursorDB.execute ("SELECT pid FROM read_historical WHERE unix_end IS NULL ORDER BY unix_start DESC LIMIT 1")
					reading = cursorDB.fetchone()
					logging.info("Last read get from sqlite database: {} ".format(reading))
					if reading is None:
						logging.info("There is no read to finish...")
					else:
						os.system("sudo kill -10 {}".format(reading[0]))
						logging.info('Killing the process with pid: {}'.format(reading[0]))

				except Exception as e:
					logging.info("Error trying to connect to access the last line at historical")
					sqliteErrorHandler_bt(e,db,logging)
			# --------------------------------------------------------------------------------------------------------------------
			# Commando to execute sensor reader script
			elif data_received == 'e':
				logging.info("Command to begin a read")
				try:
					cursorDB.execute ("SELECT pid FROM read_historical WHERE unix_end IS NULL ORDER BY unix_start DESC LIMIT 1")
					reading = cursorDB.fetchone()
					logging.info("Last read get from sqlite database: {} ".format(reading))
					if reading is None:
						os.system("sudo nice -20 python sensor_water_db.py > log_exec.txt &")
						logging.info("Starting a new process to read")

				except Exception as e:
					logging.info("Error trying to connect to access the last line at historical")
					sqliteErrorHandler_bt(e,db,logging)
			# --------------------------------------------------------------------------------------------------------------------
			# Commando to read the historic
			elif data_received == 'h':
				logging.info("Command to read historic")
				try:
					all_historical_data = []
					logging.info("Setting timeout from socket to {} and setting historic loop flag".format(historic_loop_countdown))
					count_for_lists = 0
					historic_loop = True
					read_loop = False
					chart_loop = False
					client_sock.settimeout(historic_loop_countdown)
					cursorDB.execute ("SELECT * FROM read_historical")
					reading = cursorDB.fetchall()
					logging.info("Get all historical from sqlite database")
					for row in reading:
						all_historical_data.insert(0,row)	
					all_historical_data.insert(len(all_historical_data),"finalhistoric\n");
					all_historical_data.insert(0,"initialhistoric\n");		

				except Exception as e:
					logging.info("Error trying to connect to access the historical")
					sqliteErrorHandler_bt(e,db,logging)
			# --------------------------------------------------------------------------------------------------------------------
			# Commando to delete from historic
			elif data_received_with_id == "d":
				logging.info("Command to delete id:{} historic".format(id_read))
				try:
					cursorDB.execute ("""
					DELETE 
					FROM sensor_data 
					WHERE 
					unix BETWEEN
					(SELECT unix_start FROM read_historical WHERE id={})
					AND
					(SELECT unix_end FROM read_historical WHERE id={});
					""".format(id_read,id_read))
					db.commit()
					if cursorDB.rowcount > 0:
						cursorDB.execute ("DELETE FROM read_historical WHERE id={} LIMIT 1".format(id_read))
						db.commit()
						if cursorDB.rowcount > 0:
							cursorDB.execute ("SELECT * FROM read_historical")
							reading = cursorDB.fetchall()
							logging.info("Get all historical from sqlite database")
							for row in reading:
								all_historical_data.insert(0,row)	
							all_historical_data.insert(len(all_historical_data),"finalhistoric\n");
							all_historical_data.insert(0,"initialhistoric\n");	
					
				except Exception as e:
					logging.info("Error trying to connect to delete some line from historical")
					sqliteErrorHandler_bt(e,db,logging)
			# --------------------------------------------------------------------------------------------------------------------
			# Commando to create a chart
			elif data_received_with_id == "c":	
				logging.info("Command to chart id:{} from historic".format(id_read))
				try:
					logging.info("Setting timeout from socket to {} and setting chart loop flag".format(chart_loop_countdown))
					all_chart_data = []
					count_for_lists = 0
					historic_loop = False
					read_loop = False
					chart_loop = True
					client_sock.settimeout(chart_loop_countdown)
					cursorDB.execute("""
					SELECT *,{} 
					FROM sensor_data 
					WHERE 
					unix BETWEEN 
					(SELECT unix_start FROM read_historical WHERE id={}) 
					AND 
					(SELECT unix_end FROM read_historical WHERE id={})
					""".format(id_read,id_read,id_read))
					reading = cursorDB.fetchall()					
					logging.info("Get all chart from sqlite database")
					for row in reading:
						all_chart_data.insert(0,row)			
					all_chart_data.insert(len(all_chart_data),"fin,{}\n".format(id_read));
					all_chart_data.insert(0,"ini,{}\n".format(id_read));
				except Exception as e:
					logging.info("Error trying to connect to get some chart data")
					sqliteErrorHandler_bt(e,db,logging)
			# --------------------------------------------------------------------------------------------------------------------	
			# Commando to shutdown raspberry
			elif data_received == "s":
				logging.info("Command to shutdown raspberry pi")
				#os.system("sudo shutdown -h now")
				print "sudo shutdown -h now"
				break
			# --------------------------------------------------------------------------------------------------------------------	
		except bluetooth.btcommon.BluetoothError as error:
			#logging.error("Bluetooth Error: {}".format(error))
			# Timeout catcher
			x = 1

		# -------------------------------------------------------------------------------
		# loop that send sensor data ----------------------------------------------------
		if read_loop:
			try:
				cursorDB.execute ("SELECT * FROM sensor_data ORDER BY unix DESC LIMIT 1")
				reading = cursorDB.fetchone()
				db.commit()
				
				data_to_send = "{};{};{}\r\n".format(reading[0],reading[1],reading[2])

			except Exception as e:
				logging.info("Error trying to read the DB, to send on time data")
				logging.error("sqlite Error: {}".format(str(e)))
				logging.warn("Database rollback")
				db.rollback()

			try:
				client_sock.send(data_to_send)

			except bluetooth.btcommon.BluetoothError as error:
				logging.warn("Could not connect: {}; Retrying...".format(error))
				#server_sock, port = bluetoothConnection()
				client_sock, client_info = server_sock.accept()
				client_sock.settimeout(read_loop_countdown)
				logging.info("Accepted connection from {}".format(client_info))		
		# -------------------------------------------------------------------------------
		# loop that send historic data ----------------------------------------------------
		elif historic_loop:
			try:

				row = all_historical_data[count_for_lists]
				if row == "initialhistoric\n" or row == "finalhistoric\n":
					client_sock.send(row)
				else:
					client_sock.send("{},{},{},{},{},{}\n".format(row[0],row[1],row[2],row[3],row[4],row[5]))

				if count_for_lists==(len(all_historical_data)-1):
					count_for_lists = 0
				else:	
					count_for_lists=count_for_lists+1

			except bluetooth.btcommon.BluetoothError as error:
				logging.warn("Could not connect: {}; Retrying...".format(error))
				#server_sock, port = bluetoothConnection()
				client_sock, client_info = server_sock.accept()
				client_sock.settimeout(historic_loop_countdown)
				logging.info("Accepted connection from {}".format(client_info))		
		# -------------------------------------------------------------------------------	
		# loop that send chart data ----------------------------------------------------
		elif chart_loop:
			try:
				row = all_chart_data[count_for_lists]

				if "ini" in row or "fin" in row:
					client_sock.send(row)
				else:
					client_sock.send("{},{},{},{}\n".format(row[0],row[1],row[2],row[3]))

				if count_for_lists==(len(all_chart_data)-1):
					count_for_lists = 0
				else:	
					count_for_lists=count_for_lists+1

			except bluetooth.btcommon.BluetoothError as error:
				logging.warn("Could not connect: {}; Retrying...".format(error))
				#server_sock, port = bluetoothConnection()
				client_sock, client_info = server_sock.accept()
				client_sock.settimeout(chart_loop_countdown)
				logging.info("Accepted connection from {}".format(client_info))	

		# -------------------------------------------------------------------------------	
		# check if the process are killed
		if stop:
			logging.warn('Breaking the loop!')
			break
		# -------------------------------------------------------------------------------	

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

