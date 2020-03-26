#!/usr/local/bin/python2.7

import traceback
import mysql.connector

class Database():
	"""docstring for Cardevice"""
	def __init__(self,user,password,host,port):
		self.user = user
		self.password = password
		self.host = host
		self.port = port
	
		# if self.connection.is_connected():
		# 	print('Connected to MySQL database')

	"""create database connection"""
	def getconnection(self):
		self.connection = mysql.connector.connect(user=self.user,password=self.password, \
					 host=self.host,port=self.port)
		self.cursor = self.connection.cursor()
		#if self.connection.is_connected():
		#	print('Connected to MySQL database')
                return self.connection

	"""get data from database"""
	def executequery(self,sqlselect):
		self.cursor.execute(sqlselect)
		return self.cursor.fetchall()		

	"""insert multiple entries to a table"""
        def executemany(self,sql,data):
            try:
                self.cursor.executemany(sql,data)
                self.connection.commit()
                return True
            except:
                traceback.print_exc()
                self.connection.rollback()
                print "SQL error:", sys.exc_info()[0]
                return False
            return False

	"""insert data"""
	def insertdata(self, query, value):

		self.query = query
		self.value = value

		#print(self.query,self.value)
		
		try:
			self.cursor.execute(self.query, self.value)
			self.connection.commit()
		except:
			self.connection.rollback()
			
	def closedatabaseconnection(self):
		self.cursor.close()
		self.connection.close()


