# -*- coding: utf-8 -*-
# -----------------------------------------------------
import time
import sys
import os
import bluetooth
from bluetooth import *
import MySQLdb
from useful import *
import signal
# ------------------------------------------------------
# Conexao bluetooth
server_sock=BluetoothSocket( RFCOMM )
server_sock.bind(("",PORT_ANY))
server_sock.listen(1)

port = server_sock.getsockname()[1]

uuid = "94f39d29-7d6d-437d-973b-fba39e49d4ee"

advertise_service( server_sock, "SampleServer",
                   service_id = uuid,
                   service_classes = [ uuid, SERIAL_PORT_CLASS ],
                   profiles = [ SERIAL_PORT_PROFILE ], 
 #                  protocols = [ OBEX_UUID ] 
                    )
print("Waiting for connection on RFCOMM channel %d" % port)
client_sock, client_info = server_sock.accept()
client_sock.settimeout(0.9)
print("Accepted connection from ", client_info)
# ------------------------------------------------------
# Conexao no banco
try:
	db = MySQLdb.connect("localhost", "wiki", "eeyrfxfe", "WATER_SENSOR")
	cursorDB = db.cursor()
except Exception as e:
	print "\nError trying to connect to database"
	mysqlErrorHandler(e,db,pi)
# --------------------------------------------------------
#Funcao para poder dar kill e finalizar corretamente o script
stop = False
def handler(number, frame):
	global stop
	stop = True
signal.signal(signal.SIGUSR1, handler)
# --------------------------------------------------------
try:
    while True:
    	
		try:
			data_received = client_sock.recv(1024)
			if data_received == 'k':
				print "KILL"
			elif data_received == 'e':
				print "EXE"
		except BluetoothError as e:
			#print e
			x = 1

		try:
			cursorDB.execute ("SELECT * FROM sensor_data ORDER BY unix DESC LIMIT 1")
			reading = cursorDB.fetchone()
			db.commit()
			
			data_to_send = "{};{};{}\r\n".format(reading[0],reading[1],reading[2])

		except Exception as e:
			print "Error trying to read the DB"
			db.rollback()

		try:
			client_sock.send(data_to_send)
		
		except bluetooth.btcommon.BluetoothError as error:
			print "Could not connect: ", error, "; Retrying..."
			client_sock, client_info = server_sock.accept()
			
		#time.sleep(0.9)
		if stop:
			break


except KeyboardInterrupt:
    pass
# ------------------------------------------------------
print("disconnected")
db.rollback()
db.close()	
client_sock.close()
server_sock.close()
print("all done")
# -----------------------------------------------------
# Iniciando e esperando uma conexão bluetooth

