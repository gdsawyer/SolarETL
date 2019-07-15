#!/usr/bin/env python3

'''

	enphaseDataLoad -- retrieve Enphase data
	2019-03-14

	This is the complete ETL program for Enphase data.  This is 
	mainly E and L, there's no T going on here anywhere.

'''

import sys
import os
import requests
import json
import datetime
import time
import argparse
import psycopg2
from psycopg2 import IntegrityError;
from apiAuthParameters import apiAuth


def insertSummary(dbConnection, sourceData, targetColumnNames, targetColumnMap):
	#
	#	This will basically format and execute the SQL to insert the summary
	#	data into the summary table
	#
	
	sqlValuesList = []
	for tgtColName in targetColumnNames:
		
		if type(sourceData[targetColumnMap[tgtColName]]) == int:
			sqlValuesList.append(str(sourceData[targetColumnMap[tgtColName]]))
		else:
			#
			#	We're a string, so we'll need to quote this with a single quote
			#
			sqlValuesList.append("'{0}'".format(sourceData[targetColumnMap[tgtColName]]))

	insertSql = "INSERT INTO energy.generation_daily_summary ({0}) VALUES ({1})".format(",".join(targetColumnNames), ",".join(sqlValuesList))
	insertCursor = dbConnection.cursor()
	insertCursor.execute(insertSql)
	insertCursor.close()
	#dbConnection.commit()


def insertDetail(dbConnection, generationDetailData, targetColumnNames, targetColumnMap, forDate):
	#
	#	This will insert the details, which are transmitted I believe as an array
	#	of dictionaries.  Each value represents a five-minute period of the generation
	#	day (hours of light that can stimulate the panels), and we will build up
	#	a big list of values.
	#
	
	insertValuesList = []
	for generationInterval in generationDetailData:
		dataList = []
		#
		#	First, we need to check the date in the data, make sure we are not attempting to 
		#	load data for any date other than the current loop date.  We have seen some data that 
		#	are not kosher that way.  If we do detect such a date we will skip over because it 
		#	usually means the data ran away and crossed over at midnight. So just iterate the 
		#	loop.  (Of course the way it is worded is to test to see if the date is equal to 
		#	our loop data date, and then process. That way, no unstructured bounce.)
		#
		
		if datetime.date.fromtimestamp(generationInterval['end_at']) == forDate:
			
			for insertColumn in targetColumnNames:
			
				if insertColumn == "sample_datetime":
					dataList.append("'{0}'".format(datetime.datetime.fromtimestamp(generationInterval[targetColumnMap[insertColumn]]).isoformat()))
			
				elif insertColumn == 'generation_date':
					dataList.append("'{0}'".format(datetime.datetime.fromtimestamp(generationInterval[targetColumnMap[insertColumn]]).date().isoformat()))
			
				else:
					dataList.append(str(generationInterval[targetColumnMap[insertColumn]]))
			
		
			#
			#	Now we should have a value list that has all the observations for a day.
			#	this should now be joined into a huge values clause, which will be embedded 
			#	into the SQL.  This should look something like this:
			#
	
			insertValuesList.append("({0})".format(",".join(dataList)))
		
		#
		#	which will in turn be inserted into the SQL
		#
		
		
	
	insertSql = "INSERT INTO energy.generation_detail ({0}) VALUES {1}".format(",".join(targetColumnNames), ",".join(insertValuesList))
		
	#
	#	Snag us a cursor, insert the data, then go home.
	#	
	insertCursor = dbConnection.cursor()
	insertCursor.execute(insertSql)
	insertCursor.close()
		
	
			
def main(targetDb,baseDataDate, daysToCollect ):
	
	#
	#	Initialize the ETL mapping dictionaries.  
	#
	#	We will be doing this from the point of view of the target tables.  The first structure
	#	is the target table column lists, a dictionary keyed by the target table name, with
	#	a value of the list of the target column names.  This will be cycled through to get actual
	#	data.  
	#
	#	The second structure maps the target table columns to the source data fields.  Again, the
	#	primary key is the target table name, the value is a dictionary that is keyed by the target
	#	column name with a value of the source field name.
	#
	
	tableColumnList = {"generation_daily_summary": ["generation_date","inverter_count","energy_production_watthr","energy_lifetime_watthr"],
					"generation_detail" : ["sample_datetime","generation_date","power_watts","energy_watthr"]}

	tableColumnMap = {"generation_daily_summary" : {"generation_date" : "summary_date", 
												"inverter_count":"modules",
												"energy_production_watthr":"energy_today",
												"energy_lifetime_watthr":"energy_lifetime"},
				"generation_detail" : {"sample_datetime" : "end_at",
			                           "generation_date":"end_at",
								       "power_watts" : "powr",
								       "energy_watthr" : "enwh"}}	
									   
	#
	#	We will establish a connection to the DB at this point, which will be passed to the 
	#	insert routines
	#
	
	#targetDb = psycopg2.connect(host="localhost", user="gdsawyer", database="gdsawyer")
	
	#
	#	API parameters are the basic information we need to transmit to Enphase to get access
	#	to our data through the API
	#

	apiParameters = apiAuth()

	#
	#	Initialize the data date and the incriment for the retreival loop
	#
	#	We are hard-coding this for the moment, we will get the actual dates
	#	from the parameter to this function.
	#
	timeIncrement = datetime.timedelta(days=1)

	#
	#	M A I N   W O R K   L O O P
	#
	#	This loop will cycle through the dates.  For testing purposes
	#	this loop will at the day after the baseDataDate initialized above.
	#	Normally, though the end will either be "yesterday"  (today - 1day)
	#	or an explicit end date.
	#
	#	We will also take into account timing.  The Enphase standard says
	#	we cannot execute more than 10 hits per minute. This means, in effect,
	#	one API call every 10 seconds.  But we are doing two calls per data day
	#	so we will have to modify that to be 12 seconds.  To calculate that
	#	we will take the current time (via the time module, NOT datetime.time)
	#	and calculate the next call time at the top of the loop.  After we have 
	#	pulled the data and loaded it, we can then calculate a sleep period and 
	#	sleep until we can make the next call.  
	#
	#	We will also call the loader routine.
	#



	dataDate = baseDataDate


	while dataDate < baseDataDate + daysToCollect:

		print("Now Processing:  ", dataDate)
		#
		#	Initialize the date parameter in the API parameter dictionary, 
		#	and set the next call time as being 13 seconds from now.
		#	We are using int on the time because time.time() will return 
		#	fractinal seconds.  (The return value is a Unix epoch time)
		#	We will add 15 to that.
		#
		apiParameters['summary_date'] = dataDate.strftime('%Y-%m-%d')
	
		nextCallTime = int(time.time()) + 15
	
		productionSummary = requests.get("https://api.enphaseenergy.com/api/v2/systems/6308/summary", params=apiParameters)
		productionData = json.loads(productionSummary.text)
		insertSummary(targetDb, productionData, tableColumnList['generation_daily_summary'], tableColumnMap['generation_daily_summary'])
	
		

		#
		#	now pull the five minute increments
		#
		#	First we need to delete out the summaryDate key from the parameter dictionary,
		#	and replace it with the start_at parameter.  This will be midnight on the day
		#	we want to pull.  We need to convert the dataDate to an epoch before we make the 
		#	call.  
		#
		#	After we make the pull, we will load the data, then calculate the delay
		#
	
		del apiParameters['summary_date']
		apiParameters['start_at'] = str(int(time.mktime(dataDate.timetuple())))
		productionDetail = requests.get("https://api.enphaseenergy.com/api/v2/systems/6308/stats", params=apiParameters)
	
		#
		#	LOAD the detail data here.  First pull out and normalize the json data, then transmit 
		#	the interval data out of the big json to the insert function.
		#
	
		productionDetailData = json.loads(productionDetail.text)
		insertDetail(targetDb, productionDetailData['intervals'], tableColumnList['generation_detail'], tableColumnMap['generation_detail'], dataDate)
		targetDb.commit()
	
	
		delaySeconds = nextCallTime - time.time()
	
	
		#
		#	At this point we're done, we intiate the delay
		#	if necessary.
		#
	
		if delaySeconds > 0:
			time.sleep(delaySeconds)
	
	
		#
		#	Now calculate the next data date and loop back
		#
		dataDate = dataDate + timeIncrement





if __name__ == '__main__':
	
	parser = argparse.ArgumentParser(description="Retrieve solar production data from Enphase and load into database")

	parser.add_argument('-v','--version', action='version',version='%(prog)s Version v1.0')
	parser.add_argument("--start-date", 
		action='store', 
		default=(datetime.date.today() - datetime.timedelta(days=1)).isoformat(), 
		dest='startDate',
		help='First data date to collect data, defaults to "yesterday" if not specified')

	groupv = parser.add_mutually_exclusive_group(required=False)
	groupv.add_argument('--end-date', 
		action='store', 
		default='1970-01-01', 
		dest='endDate',
		help='Last data date to collect data; there is no default if not specified.  Cannot be used with --date-window')
	groupv.add_argument('--date-window', 
		action='store', 
		dest='dateWindow',
		nargs=2,
		default=["1",'days'],
		metavar=('N', 'UNITS'),
		help='Number of days or weeks to collect data starting at Start Date, where N is an integer number and \
			  UNITS is either "days" or "weeks", defaults to "1 days" if not specified. \
		      Cannot be used with --end-date')

	#
	#	Get the arguments and process
	#

	oneDay = datetime.timedelta(days=1)
	argv = parser.parse_args()

	#
	#	First set the start date.  This will go directly to a datetime date object
	#	We will validate to make sure that we have an acceptable value, start date
	#	must be "yesterday" or before.  If not, exit the program.  NOTE that if
	#	startDate is not in a proper ISO format, the date object instantiation 
	#	will blow up hard.  We are not trapping this error at this time.
	#

	startDate = datetime.date.fromisoformat(argv.startDate)

	if (datetime.date.today() - startDate).days < 0:
		parser.exit('Invalid Start Date,' + startDate.isoformat() + ' must be earlier than today')


	#
	#	Now process the endDate and Interval.  We will first deal with the interval
	#	validate both the number of time units and the time units itself.  If it passes
	#	we will then check to see if the endDate is default (The Epoch), and if
	#	it is, we will then instantiate an interval (timedelta) object.  If 
	#	endDate is not The Epoch, it means that someone has entered something.
	#	This being the case, we will form the interval from the two dates.
	#


	
	if not argv.dateWindow[0].isdigit():
		parser.exit("First date-window parameter Interval Period (" + argv.dateWindow[0] + ") must be just a number (integer)")
	elif argv.dateWindow[1].lower() not in {'day','days','week','weeks'}:
		parser.exit("Second date-window parameter Interval Units (" + argv.dateWindow[1] + ") is not a valid date window unit, must be either 'day[s]' or 'week[s]'") 
	elif argv.endDate == '1970-01-01':
		if 'day' in argv.dateWindow[1]:
			dataDaysToProcess = datetime.timedelta(days=int(argv.dateWindow[0]))
		else:
			dataDaysToProcess = datetime.timedelta(weeks=int(argv.dateWindow[0]))
	
		if (startDate + dataDaysToProcess) > datetime.date.today():
			parser.exit("date-window interval of {0} days added to start-date {1} goes past today into the future.".format(dataDaysToProcess.days, startDate.isoformat() ))

	else:

		#
		#	Note:  like startDate, if endDate isn't in an ISO 8601 format this will 
		#	blow up in a messy way, and we are not trapping it at this point.  We
		#	also validate the end date to make sure it is within the bounds by
		#	checking to make sure the end date is not before the start date
		#	(the interval will have a negative value), or that the end date
		#	isn't in the future (actually "today" or in the future)
		#
		endDate = datetime.date.fromisoformat(argv.endDate)
		#
		#	We need to add one day to this calculation because we want it 
		#	to process all the days specified.  We are loading both 
		#	startDate and endDate, and the delta of those is value
		#	it takes to add to start to get to end.  We need it to 
		#	be end+ 1 day.
		#
		dataDaysToProcess = endDate - startDate + oneDay
	
		if dataDaysToProcess.days <0 or (datetime.date.today() - endDate).days <= 0:
			parser.exit("End-date {0} must be between {1} and {2} inclusive".format(endDate.isoformat(), startDate.isoformat(), (datetime.date.today() - oneDay).isoformat()))
	
	#
	#	If we have lived this far, we get to play with databases next.  
	#
	#	This will perform the vladation within the database to see if we
	#	have any data in there that conforms to the end date.
	#

	energyDb = psycopg2.connect(host='localhost', user='gdsawyer', database='gdsawyer')
	qCursor = energyDb.cursor()
	qCursor.execute("SELECT count(*) AS rec_count FROM energy.generation_daily_summary WHERE generation_date <@ DATERANGE(%s, %s)", \
		(startDate, (startDate + dataDaysToProcess) ))


	testRowCount = qCursor.fetchone()
	energyDb.commit()
	qCursor.close()

	if testRowCount[0] > 0:
		parser.exit("There is already data loaded for the date range {0} to {1}".format(startDate.isoformat(), (startDate + dataDaysToProcess).isoformat()))


	#
	#	Now we are ready to call Main()
	#
	
	print("Ready to go starting on {0} and ending on {1} (interval {2} days)".format(startDate.isoformat(), (startDate + dataDaysToProcess - oneDay).isoformat(), dataDaysToProcess.days))
	
	main(energyDb, startDate, dataDaysToProcess)
