# -*- coding: utf-8 -*-
# -----------------------------------------------------
import bluetooth
import logging
from bluetooth import *

def mysqlErrorHandler(e,db,pi,logging):
	logging.error("MySQL Error: %s" % str(e))
	db.rollback()
	logging.warn('Database rollback')
	db.close()
	logging.warn('Database close connection')		
	pi.stop()
	logging.warn('PIGPIO stop')
	exit()

def bluetoothConnection():
	server_sock = BluetoothSocket( RFCOMM ) #Constructs a socket for RFCOMM service.
	server_sock.bind(("",PORT_ANY)) #Server binds the script on host '' to any port.
	server_sock.listen(1) #Server listens to accept 1 connection at a time.
	port = server_sock.getsockname()
	#print port
	uuid = "94f39d29-7d6d-437d-973b-fba39e49d4ee"
	advertise_service( server_sock, "SampleServer{}".format(port[1]), service_id = uuid, service_classes = [ uuid, SERIAL_PORT_CLASS ],profiles = [ SERIAL_PORT_PROFILE ], 
 #                  protocols = [ OBEX_UUID ] 
                    )
	return server_sock,port
