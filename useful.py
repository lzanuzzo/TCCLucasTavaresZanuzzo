# -*- coding: utf-8 -*-
# -----------------------------------------------------
def mysqlErrorHandler(e,db,pi):
	print "\nMySQL Error: %s" % str(e)
	db.rollback()
	db.close()		
	pi.stop()
	exit()