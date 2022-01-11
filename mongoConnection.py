#!/usr/bin/env python

import pymongo
import os

from pymongo import MongoClient
from os import environ, path
from dotenv import load_dotenv


basedir = path.abspath(path.dirname(__file__))
load_dotenv(path.join(basedir, 'EnvFile.env'))

# Provide the mongodb atlas url to connect python to mongodb using pymongo
CONNECTION_STRING = os.environ.get("CONNECTION_STRING ")
DATABASE = os.environ.get("DATABASE")


#-----------------------------------------CORE FUNCTIONS-----------------------------------------

def getConnection():

	try:
		client = MongoClient(CONNECTION_STRING)
		return client
	except pymongo.errors.ServerSelectionTimeoutError as error:
		print ("Error with MongoDB connection: " + error)
	except pymongo.errors.ConnectionFailure as error:
		print ("Could not connect to MongoDB: " + error)


def getClientDB(DB):
	#Create DB_MongoObject
	clientMongo = getConnection()
	DB_Object   = clientMongo[DB]
	return[clientMongo,DB_Object]


#-----------------------------------------CUSTOM FUNCTIONS-----------------------------------------

def clearDB(DBObject):
	print ("Collections to eliminate")
	print(DBObject.list_collection_names())
	for collection in DBObject.list_collection_names():
		result = DBObject[collection].drop()



def clear_Prueba1():
	#Clear database
	DB_Object   = getClientDB(DATABASE)[1]
	clearDB(DB_Object)
	print("DATABASE CLEARED")


def insert_in_prueba1():
	result_list = []

	#Create DB_MongoObject
	clientMongo = getClientDB(DATABASE)[0]
	DB_Object   = getClientDB(DATABASE)[1]

	col = DB_Object["prueba1"]

	col.insert_one({"key1":"val1","key2":"val2"})

	clientMongo.close()

def update_in_prueba1():

	#Create DB_MongoObject
	clientMongo = getClientDB(DATABASE)[0]
	DB_Object   = getClientDB(DATABASE)[1]

	col = DB_Object["prueba1"]

	col.update_one({"key1" : "val1" },{"$set": {"key2" : "val999" }})
	clientMongo.close()

def delete_in_prueba1():

	#Create DB_MongoObject
	clientMongo = getClientDB(DATABASE)[0]
	DB_Object   = getClientDB(DATABASE)[1]

	col = DB_Object["prueba1"]

	col.delete_one({})
	clientMongo.close()


def print_prueba1():

	result_list = []

	#Create DB_MongoObject
	clientMongo = getClientDB(DATABASE)[0]
	DB_Object   = getClientDB(DATABASE)[1]


	documents = DB_Object.get_collection("prueba1")

	for doc in documents.find({}):
		result_list.append(doc)

	clientMongo.close()

	print(result_list)

