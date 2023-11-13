# flake8: noqa
import os
import requests
from opentelemetry import trace
from opentelemetry.trace import Status
from opentelemetry.trace import StatusCode
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
        OTLPSpanExporter)
from opentelemetry.sdk.resources import SERVICE_NAME

import sys
import re
from lxml import etree
import logging
import datetime
from pymongo import MongoClient
from xml.sax.saxutils import escape

# To help find other directories that might hold modules or config files
binDir = os.path.dirname(os.path.realpath(__file__))

# Find and load any of our modules that we need
confDir = binDir.replace('/bin', '/conf')
libDir  = binDir.replace('/bin', '/lib')
sys.path.append(confDir)
sys.path.append(libDir)
from .etds2alma_tables import degreeCodeName, degreeLevelTracing, schools
from lib.ltstools import get_date_time_stamp
from .xfer_files import xfer_files
from lib.notify import notify

# tracing setup
JAEGER_NAME = os.getenv('JAEGER_NAME')
JAEGER_SERVICE_NAME = os.getenv('JAEGER_SERVICE_NAME')
notify.logDir = os.getenv("LOGFILE_PATH", "/home/etdadm/logs/etd_alma")

resource = Resource(attributes={SERVICE_NAME: JAEGER_SERVICE_NAME})
provider = TracerProvider(resource=resource)
otlp_exporter = OTLPSpanExporter(endpoint=JAEGER_NAME, insecure=True)
span_processor = BatchSpanProcessor(otlp_exporter)
provider.add_span_processor(span_processor)
trace.set_tracer_provider(provider)
tracer = trace.get_tracer(__name__)

almaMarcxmlTemplate = os.getenv('ALMA_MARCXML_TEMPLATE',
								"/home/runner/work/etd_alma_service/" \
								"etd_alma_service/templates/" \
								"alma_marcxml_template.xml")
dropboxUser         = os.getenv('DROPBOX_USER')
dropboxServer       = os.getenv('DROPBOX_SERVER')
privateKey          = os.getenv('PRIVATE_KEY_PATH')
dataDir             = os.getenv('DATA_DIR')
filesDir            = os.getenv('FILES_DIR')
alreadyRunRef       = f'{filesDir}/already_processed.ref'
dashLink            = 'https://nrs.harvard.edu/urn-3:HUL.InstRepos:'
notifyJM            = False
jobCode             = 'etds2alma'
mongoUrl            = os.getenv('MONGO_URL')
mongoDbName         = os.getenv('MONGO_DB_NAME')
mongoDbCollection   = os.getenv('MONGO_DB_COLLECTION')

metsDmdSecNamespace = '{http://www.loc.gov/METS/}'
metsDimNamespace    = '{http://www.dspace.org/xmlns/dspace/dim}'
#-marcXmlNamespace    = '{http://www.loc.gov/MARC21/slim}'

yyyymmdd          = get_date_time_stamp('day')
yymmdd            = yyyymmdd[2:]
xmlCollectionFileName = f'AlmaDelivery_{yyyymmdd}.xml'

reTheTitle = re.compile('"?(the) .*', re.IGNORECASE)
reAnTitle  = re.compile('"?(an) .*', re.IGNORECASE)
reATitle   = re.compile('"?(a) .*', re.IGNORECASE)

# To map numeric month to named month
months = ['not_used', 'January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']

# To wrap the xml records in a collection
xmlStartCollection = """
<collection xmlns="http://www.loc.gov/MARC21/slim"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://www.loc.gov/MARC21/slim http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd">
"""

xmlEndCollection = "</collection>"

FEATURE_FLAGS = "feature_flags"
ALMA_FEATURE_FORCE_UPDATE_FLAG = "alma_feature_force_update_flag"
ALMA_FEATURE_VERBOSE_FLAG = "alma_feature_verbose_flag"
INTEGRATION_TEST = os.getenv('MONGO_DB_COLLECTION_ITEST', 'integration_test')
ALMA_TEST_BATCH_NAME = os.getenv('ALMA_TEST_BATCH_NAME','proquest2023071720-993578-gsd')

"""
This the worker class for the etd alma service.

Since: 2023-05-23
Author: cgoines
"""


class Worker():
    version = None
    logger = logging.getLogger('etd_alma')

    def __init__(self):
        self.version = os.getenv("APP_VERSION", "0.0.1")

    def get_version(self):
        return self.version

    # this should be replaced by a call to test alma sftp
    # and exercised in the tests
    def call_api(self):
        url = "https://dash.harvard.edu/rest/test"
        r = requests.get(url)
        return r.text
	
    @tracer.start_as_current_span("send_to_alma_worker")
    def send_to_alma(self, message):  # pragma: no cover
        force = False
        verbose = False
        integration_test = False
        current_span = trace.get_current_span()
        if FEATURE_FLAGS in message:
            feature_flags = message[FEATURE_FLAGS]
            if (ALMA_FEATURE_FORCE_UPDATE_FLAG in feature_flags and
                feature_flags[ALMA_FEATURE_FORCE_UPDATE_FLAG] == "on"):
                force = True
            if (ALMA_FEATURE_VERBOSE_FLAG in feature_flags and
                feature_flags[ALMA_FEATURE_VERBOSE_FLAG] == "on"):
                verbose = True
        if (INTEGRATION_TEST in message and
            message[INTEGRATION_TEST] == True):
            integration_test = True
            self.logger.info('running integration test for alma service')	
            current_span.add_event("running integration test for alma service")
        current_span.add_event("sending to alma worker main")
        self.logger.info('sending to alma worker main')
        self.send_to_alma_worker(force, verbose, integration_test)
        self.logger.info('complete')
        return True
		
    @tracer.start_as_current_span("send_to_alma_worker_main")
    def send_to_alma_worker(self, force = False,
							verbose = False, integration_test = False):  # pragma: no cover
        current_span = trace.get_current_span()
        current_span.add_event("sending to alma dropbox")
        global notifyJM
        wroteXmlRecords = False

        collectionName = None
        if integration_test:
            collectionName = INTEGRATION_TEST
        else:
            collectionName = mongoDbCollection

        # Connect to mongo
        try:
            mongo_client = MongoClient(mongoUrl, maxPoolSize=1)
            mongoDb = mongo_client[mongoDbName]
        except Exception as err:
            self.logger.error("Error: unable to connect to mongodb", exc_info=True)

	    # Create a notify object, this will also set-up logging and
        # logFile  = f'{logDir}/{jobCode}.{yymmdd}.log'
        notifyJM = notify('monitor', jobCode, None)
		
        # Let the Job Monitor know that the job has started
        notifyJM.log('pass', 'Process ETDs from Proquest to Alma', verbose)
        notifyJM.report('start')

	    # Build batchesIn by looking at the data directory
        batchesIn = []
        for batch in os.listdir(dataDir + '/in'):
          schoolMatch = re.match(r'proquest\d+-\d+-(\w+)', batch)
          if schoolMatch:
              school = schoolMatch.group(1)
              batchesIn.append([school, batch])

        # Start xml record collection output file
        xmlCollectionFile = xmlCollectionFileName
        if integration_test:
            xmlCollectionFile = f'AlmaDeliveryTest_{yyyymmdd}.xml'
            schoolMatch = re.match(r'proquest\d+-\d+-(\w+)', ALMA_TEST_BATCH_NAME)
            if schoolMatch:
                school = schoolMatch.group(1)
                batchesIn = [[school, ALMA_TEST_BATCH_NAME]]			
        xmlCollectionOut = open(xmlCollectionFile, 'w')
        xmlCollectionOut.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        xmlCollectionOut.write(f'{xmlStartCollection}\n')

        # Process batches
        recordsWereUpdated = False
        numRecordsUpdated = 0
        for (school, batch) in batchesIn:
            batchOutDir      = f'{dataDir}/out/{batch}'
            skipBatch = False

            # Do not re-run a processed batch unless forced #- test
            if (not integration_test):
                if ((not force) and (os.path.exists(alreadyRunRef))):
                    with open(alreadyRunRef, 'r') as alreadyRunTable:
                        for line in alreadyRunTable:
                            if f'Alma {batch} {school}' == line.rstrip():
                                notifyJM.log('fail', f'Batch {batch} has already been run. Use force flag to re-run.', True)
                                current_span.set_status(Status(StatusCode.ERROR))
                                current_span.add_event(f'Batch {batch} has already been run. Use force flag to re-run.')
                                skipBatch = True
                                continue
            if skipBatch:
                continue
            # Let the Job Monitor know that the job has started
            notifyJM.log('pass', f'Process batch {batch} for school {school}', verbose)

            # Check for mets file and mapfile
            metsFile = f'{dataDir}/in/{batch}/mets.xml'
            if not os.path.exists(metsFile):
                notifyJM.log('fail', f"{metsFile} not found", True)
                notifyJM.log('fail', f'skippping batch {batch} for school {school}', True)
                current_span.set_status(Status(StatusCode.ERROR))
                current_span.add_event(f'skippping batch {batch} for school {school}')
                self.logger.error(f'skippping batch {batch} for school {school}')
                continue

            mapFile = f'{batchOutDir}/mapfile'
            if not os.path.exists(mapFile):
                notifyJM.log('fail', f"{mapFile} not found", True)
                notifyJM.log('fail', f'skippping batch {batch} for school {school}', True)
                current_span.set_status(Status(StatusCode.ERROR))
                current_span.add_event(f'skippping batch {batch} for school {school}')
                self.logger.error(f'skippping batch {batch} for school {school}')
                continue

            # Get needed data from mets file
            marcXmlValues = getFromMets(metsFile, verbose)
	
            if marcXmlValues:

                # And try to get Dash ID from map file
                with open(mapFile) as mapFileIn:
                    mapFileContents = mapFileIn.read()
                    match = re.match(r'submission_\d+ \d+/(\d+)', mapFileContents)
                if match:
                    marcXmlValues['dash_id'] = match.group(1)
                else:
                    marcXmlValues['dash_id'] = False
                    notifyJM.log('warn', f"Dash ID was not found in {mapFile}", True)
	
                # Write marc xml record in batch directory
                marcXmlRecord = False
                try:
                    marcXmlRecord = writeMarcXml(batch, batchOutDir, marcXmlValues, verbose)
                except Exception as err:
                    self.logger.error(f"Writing MARCXML record for {batch} for {school} failed, skipping", exc_info=True)
                    notifyJM.log('fail', f"Writing MARCXML record for {batch} for {school} failed, skipping", True)
                    current_span.set_status(Status(StatusCode.ERROR))
                    current_span.add_event(f'Writing MARCXML record for {batch} for {school} failed, skipping')
                    continue

                # And then write xml record to collection file
                if marcXmlRecord:
                    xmlCollectionOut.write(marcXmlRecord)
                    wroteXmlRecords = True
                    numRecordsUpdated = numRecordsUpdated + 1
                    # Update processed reference file
                    if (not integration_test):
                        with open(alreadyRunRef, 'a+') as alreadyRunFile:					
                            alreadyRunFile.write(f'Alma {batch} {school}\n')
					# Update mongo
                    proquestId = marcXmlValues['proquestId']
                    schoolAlmaDropbox = school
                    almaSubmissionStatus = "ALMA_DROPBOX"
                    insertionDate = datetime.datetime.now().isoformat()
                    lastModifiedDate = datetime.datetime.now().isoformat()
                    almaDropboxSubmissionDate = datetime.datetime.now().isoformat()
                    writeSuccess = write_record(proquestId, 
                                                schoolAlmaDropbox,
                                                almaSubmissionStatus,
                                                insertionDate,
                                                lastModifiedDate,
                                                almaDropboxSubmissionDate,
                                                collectionName, mongoDb)
                    if (not writeSuccess):
                        self.logger.error(f'Could not record proquest id {proquestId} in {batch} for school {school} in mongo')
                        notifyJM.log('fail', f"Could not record proquest id {proquestId} in {batch} for school {school} in mongo", True)
                        current_span.set_status(Status(StatusCode.ERROR))	
                        current_span.add_event(f'Could not record proquest id {proquestId} in {batch} for school {school} in mongo')
					
        # If marcxml file was written successfully, finish xml records 
	    # collection file and then send it to dropbox for Alma to load
        if wroteXmlRecords:

            xmlCollectionOut.write(f'{xmlEndCollection}\n')
            xmlCollectionOut.close()

            xfer = xfer_files(dropboxServer, dropboxUser, privateKey)
	
            if xfer.error:
                notifyJM.log('fail', xfer.error, True)

            else:
                targetFile = '/incoming/' + os.path.basename(xmlCollectionFile)
                xfer.put_file(xmlCollectionFile, targetFile)

                if xfer.error:
                    notifyJM.log('fail', xfer.error, True)
                    current_span.set_status(Status(StatusCode.ERROR))
                    current_span.add_event(xfer.error)
                    self.logger.error(xfer.error)
                else:
                    notifyJM.log('pass', f'{xmlCollectionFile} was sent to {dropboxUser}@{dropboxServer}:{targetFile}', verbose)
                    current_span.set_attribute("uploaded_identifier", marcXmlValues['proquestId'])
                    current_span.set_attribute("uploaded_file", targetFile)
                    current_span.add_event(f'{xmlCollectionFile} was sent to {dropboxUser}@{dropboxServer}:{targetFile}')
                    self.logger.debug("uploaded proquest id: " + str(marcXmlValues['proquestId']))
                    self.logger.debug("uploaded file: " + str(targetFile))

            xfer.close()

            recordsWereUpdated = True
            # Otherwise, remove file
            os.remove(xmlCollectionFile)
        else:
            xmlCollectionOut.close()
            os.remove(xmlCollectionFile)
            notifyJM.log('pass', 'No record to send to Alma', verbose)
            current_span.add_event("No record to send to Alma")
            self.logger.debug("No record to send to Alma")

        current_span.add_event(f'{numRecordsUpdated} records were updated')
        self.logger.debug(f'{numRecordsUpdated} records were updated')
        notifyJM.log('pass', f'{numRecordsUpdated} records were updated', verbose)
        notifyJM.report('complete')
        current_span.add_event("completed")
	
        # Returns True if records were updated, otherwise, return False
        return recordsWereUpdated
    

# Get data from mets file that's needed to write marc xml.
# The marcXmlValues dictionary is populated and returned.
def getFromMets(metsFile, verbose):  # pragma: no cover
	global notifyJM, jobCode
	if notifyJM == False:
		notifyJM = notify('monitor', jobCode, None)
	foundAll         = True
	marcXmlValues    = {}
	pqSubjects       = []
	subjects         = []
	advisors         = []
	committeeMembers = []
	
	# Load mets file, get the root node and then parse
	metsTree   = etree.parse(metsFile)
	rootMets   = metsTree.getroot()
	rootDmdSec = rootMets.find(f'{metsDmdSecNamespace}dmdSec')
	for dimField in rootDmdSec.iter(f'{metsDimNamespace}field'):
	
		# Dc mdschema
		if dimField.attrib['mdschema'] == 'dc':

			# Date created
			if dimField.attrib['element'] == 'date':
				if dimField.attrib['qualifier'] == 'created':
					marcXmlValues['dateCreated'] = dimField.text

			elif dimField.attrib['element'] == 'identifier':

				# Proquest ID
				if dimField.attrib['qualifier'] == 'other':
					marcXmlValues['proquestId'] = dimField.text
					
				# ORCID
				elif dimField.attrib['qualifier'] == 'orcid':
					marcXmlValues['orcid']     = dimField.text
					marcXmlValues['orcidUrl'] = f"https://orcid.org/{marcXmlValues['orcid']}"
				
			elif dimField.attrib['element'] == 'contributor':

				# Author
				if dimField.attrib['qualifier'] == 'author':
					marcXmlValues['author'] = dimField.text

				# Advisors
				if dimField.attrib['qualifier'] == 'advisor':
					advisors.append(dimField.text)
	
				# Committee members
				if dimField.attrib['qualifier'] == 'committeeMember':
					committeeMembers.append(dimField.text)
	
			# Title and title indicator 2
			elif dimField.attrib['element'] == 'title':
				marcXmlValues['title'] = dimField.text
				
				if reTheTitle.match(marcXmlValues['title']):
					marcXmlValues['titleIndicator2'] = '4'
				elif reAnTitle.match(marcXmlValues['title']):
					marcXmlValues['titleIndicator2'] = '3'
				elif reATitle.match(marcXmlValues['title']):
					marcXmlValues['titleIndicator2'] = '2'
				else:
					marcXmlValues['titleIndicator2'] = '0'
									
			# Description
			elif dimField.attrib['element'] == 'description':
				if dimField.attrib['qualifier'] == 'abstract':
					marcXmlValues['description'] = escapeStr(dimField.text)
				
			# Subjects
			elif dimField.attrib['element'] == 'subject':
				try:
					if dimField.attrib['qualifier'] == 'PQ':
						pqSubjects.append(dimField.text)
				except:
					subjects.append(dimField.text)
	
		# Thesis mdschema
		elif dimField.attrib['mdschema'] == 'thesis':
			if dimField.attrib['element'] == 'degree':
		
				# Get degree name & use it to lookup degree translation
				if dimField.attrib['qualifier'] == 'name':
					marcXmlValues['degreeName'] = dimField.text
					try:
						marcXmlValues['degreeTranslation'] = degreeCodeName[marcXmlValues['degreeName']]
					except:
						degreeCode = marcXmlValues['degreeName']
						notifyJM.log('fail', f'Degree name not found for degree code {degreeCode} in lookup table', True)
						foundAll = False
						
				# Degree date, year and maybe month
				elif dimField.attrib['qualifier'] == 'date':
					marcXmlValues['degreeDate'] = dimField.text
					marcXmlValues['degreeYear'] = marcXmlValues['degreeDate'][0:4]
					
					if len(marcXmlValues['degreeDate']) >= 7: #- not tested
						degreeMonthNumeric = int(marcXmlValues['degreeDate'][5:7])
						marcXmlValues['degreeMonth'] = months[degreeMonthNumeric]
					else:
						marcXmlValues['degreeMonth'] = ''
						
				# Degree level
				elif dimField.attrib['qualifier'] == 'level':
					marcXmlValues['degreeLevel'] = dimField.text
						
				# Department
				elif dimField.attrib['qualifier'] == 'department':
					marcXmlValues['department'] = dimField.text
						
				# School
				elif dimField.attrib['qualifier'] == 'grantor':
					marcXmlValues['school'] = dimField.text
	
		# Dash mdschema
		elif dimField.attrib['mdschema'] == 'dash':
		
			# Embargo data
			if dimField.attrib['element'] == 'embargo':
				if dimField.attrib['qualifier'] == 'until':
					marcXmlValues['embargoDate'] = dimField.text
		
	# If we found any subjects or advisors, add list to data structure
	if len(pqSubjects) > 0:
		marcXmlValues['pq_subjects'] = pqSubjects
	if len(subjects) > 0:
		marcXmlValues['subjects'] = subjects
	if len(advisors) > 0:
		marcXmlValues['advisors'] = advisors
	if len(committeeMembers) > 0:
		marcXmlValues['committeeMembers'] = committeeMembers

	# Check that we found our needed values
	for var in ('dateCreated', 'proquestId', 'author', 'degreeName', 'degreeDate', 'degreeLevel', 'title', 'school'):
		if var not in marcXmlValues:
			notifyJM.log('fail', f'Failed to find {var} in {metsFile}', True)
			foundAll = False
			
	if foundAll:
		return marcXmlValues
	else:
		return False

# Write marcxml using data passed in the marcXmlValues dictionary
def writeMarcXml(batch, batchOutDir, marcXmlValues, verbose):  # pragma: no cover
	global notifyJM, jobCode
	if notifyJM == False:
		notifyJM = notify('monitor', jobCode, None)
	removeNodes   = set()
	xmlRecordFile = f'{batchOutDir}/' + batch.replace('proquest', 'alma') + '.xml'
	
	# Load template file and swapped in variables
	marcXmlTree = etree.parse(almaMarcxmlTemplate)
	rootRecord = marcXmlTree.getroot()

#-	rootRecord = rootMarcXml.find(f'{marcXmlNamespace}record')

	for child in rootRecord.iter('controlfield', 'subfield'):
	
		# 008 controlfield
		if child.tag == 'controlfield':
			if child.attrib['tag'] == '008':
				childText = child.text.replace('YYMMDD', yymmdd)
				childText = childText.replace('DATE_CREATED_VALUE', marcXmlValues['dateCreated'])
				child.text = childText

		# datafield/subfields
		elif child.tag == 'subfield':
			parent = child.getparent()
			if parent.tag == 'datafield':
				
				# Datafield 035, Proquest ID
				if parent.attrib['tag'] == '035':
					if child.attrib['code'] == 'a':
						childText  = child.text.replace('PROQUEST_IDENTIFIER_VALUE', marcXmlValues['proquestId'])
						child.text = childText

				# Datafield 100
				elif parent.attrib['tag'] == '100':

					# Author
					if child.attrib['code'] == 'a':
						childText  = child.text.replace('AUTHOR_VALUE', marcXmlValues['author'])
						child.text = escapeStr(childText)

					# Degree name and year				
					if child.attrib['code'] == 'c':
						childText  = child.text.replace('DEGREE_NAME_VALUE', marcXmlValues['degreeName'])
						childText  = childText.replace('DEGREE_YEAR_VALUE', marcXmlValues['degreeYear'])
						child.text = childText
						
					# ORCID, update if found in the mets, remove it otherwise
					if child.attrib['code'] == '1':
						if 'orcid' in marcXmlValues:
							childText  = child.text.replace('ORCID_VALUE_URL', marcXmlValues['orcidUrl'])
							child.text = childText
						else:
							parent.remove(child)

				# Datafield 245, title and title indicator 2
				elif parent.attrib['tag'] == '245':
					if child.attrib['code'] == 'a':
						childText  = child.text.replace('TITLE_VALUE', marcXmlValues['title'])
						child.text = childText
						parentInd2 = parent.attrib['ind2'].replace('TITLE_INDICATOR_2_VALUE', marcXmlValues['titleIndicator2'])
						parent.attrib['ind2'] = parentInd2

				# Datafield 264, subfield c, date created
				elif parent.attrib['tag'] == '264':
					if child.attrib['code'] == 'c':
						childText  = child.text.replace('DATE_CREATED_VALUE', marcXmlValues['dateCreated'])
						child.text = childText

				# Datafield 500, subfield a
				elif parent.attrib['tag'] == '500':
				
					# Updated ORCID if we have it, otherwise, remove datafield
					if child.attrib['code'] == 'a':
						if 'orcid' in marcXmlValues:
							childText  = child.text.replace('ORCID_VALUE', marcXmlValues['orcid'])
							child.text = childText
						else:
							removeNodes.add(parent)

				# 502 Datafields
				elif parent.attrib['tag'] == '502':
				
					# Subfields b and d, degree name and year
					if child.attrib['code'] == 'b':
						childText  = child.text.replace('DEGREE_NAME_VALUE', marcXmlValues['degreeName'])
						child.text = childText
					elif child.attrib['code'] == 'd':
						childText  = child.text.replace('DEGREE_YEAR_VALUE', marcXmlValues['degreeYear'])
						child.text = childText

					# Subfield a for second 502, degree name, department, school and year
					elif child.attrib['code'] == 'a':
						childText = child.text.replace('DEGREE_NAME_VALUE', marcXmlValues['degreeName'])
						childText = childText.replace('DEGREE_TRANSLATION_VALUE', marcXmlValues['degreeTranslation'])
						childText = childText.replace('SCHOOL_VALUE', marcXmlValues['school'])
						childText = childText.replace('DEGREE_MONTH_VALUE', marcXmlValues['degreeMonth'])
						childText = childText.replace('DEGREE_YEAR_VALUE', marcXmlValues['degreeYear'])
						
						if 'department' in marcXmlValues:
							childText = childText.replace('DEPARTMENT_VALUE', marcXmlValues['department'])
						else:
							childText = childText.replace(', DEPARTMENT_VALUE', '')
													
						child.text = childText

				# Datafield 520, ind1 3, subfield a, description
				elif parent.attrib['tag'] == '520':
					if parent.attrib['ind1'] == '3' and child.attrib['code'] == 'a':
						childText  = child.text.replace('ABSTRACT_VALUE', marcXmlValues['description'])
						child.text = escapeStr(childText)
						
				# Datafield 653s
				elif parent.attrib['tag'] == '653':
					childText  = child.text

					# PQ subject, 0 or more					
					if childText == 'PQ_SUBJECT_VALUE':
						if 'pq_subjects' in marcXmlValues:

							# Add the first PQ Subject
							pqSubject = marcXmlValues['pq_subjects'].pop(0)
							datafield653Str = etree.tostring(parent, encoding='unicode')
							childText  = childText.replace('PQ_SUBJECT_VALUE', pqSubject)
							child.text = escapeStr(childText)

							# If there's more, add them but the list needs 
							# to be reversed to keep the order
							if len(marcXmlValues['pq_subjects']) > 0:
								marcXmlValues['pq_subjects'].reverse()
								for pqSubject in marcXmlValues['pq_subjects']:
									pqSubject = escapeStr(pqSubject)
									newDatafield653Str = datafield653Str.replace('PQ_SUBJECT_VALUE', pqSubject)
									parent.addnext(etree.fromstring(newDatafield653Str))
									
						# Remove element if we have no PQ subjects
						else:
							removeNodes.add(parent)

					# Subject, 0 or more					
					elif childText == 'SUBJECT_VALUE':
						if 'subjects' in marcXmlValues:

							# Add the first Subject
							subject = marcXmlValues['subjects'].pop(0)
							datafield653Str = etree.tostring(parent, encoding='unicode')
							childText  = childText.replace('SUBJECT_VALUE', subject)
							child.text = escapeStr(childText)

							# If there's more, add them but the list needs 
							# to be reversed to keep the order
							if len(marcXmlValues['subjects']) > 0:
								marcXmlValues['subjects'].reverse()
								for subject in marcXmlValues['subjects']:
									subject = escapeStr(subject)
									newDatafield653Str = datafield653Str.replace('SUBJECT_VALUE', subject)
									parent.addnext(etree.fromstring(newDatafield653Str))
									
						# Remove element if we have no subjects
						else:
							removeNodes.add(parent)
						
				# Datafield 720s
				elif parent.attrib['tag'] == '720':
					childText  = child.text

					# Advisor, 0 or more					
					if childText == 'ADVISOR_VALUE,':
						if 'advisors' in marcXmlValues:

							# Add the first advisor
							advisor = marcXmlValues['advisors'].pop(0)
							datafield720Str = etree.tostring(parent, encoding='unicode')
							childText  = childText.replace('ADVISOR_VALUE', advisor)
							child.text = escapeStr(childText)

							# If there's more, add them but the list needs 
							# to be reversed to keep the order
							if len(marcXmlValues['advisors']) > 0:
								marcXmlValues['advisors'].reverse()
								for advisor in marcXmlValues['advisors']:
									advisor = escapeStr(advisor)
									newDatafield720Str = datafield720Str.replace('ADVISOR_VALUE,', advisor)
									newDatafield720Str += '\n'
									parent.addnext(etree.fromstring(newDatafield720Str))
									
						# Remove element if we have no advisors
						else:
							removeNodes.add(parent)

					# Committee members, 0 or more
					elif childText == 'COMMITTEE_MEMBER_VALUE,':
						if 'committeeMembers' in marcXmlValues:

							# Add the first committee member
							committeeMember = marcXmlValues['committeeMembers'].pop(0)
							datafield720Str = etree.tostring(parent, encoding='unicode')
							childText  = childText.replace('COMMITTEE_MEMBER_VALUE', committeeMember)
							child.text = escapeStr(childText)

							# If there's more, add them but the list needs 
							# to be reversed to keep the order
							if len(marcXmlValues['committeeMembers']) > 0:
								marcXmlValues['committeeMembers'].reverse()
								for committeeMember in marcXmlValues['committeeMembers']:
									committeeMember = escapeStr(committeeMember)
									newDatafield720Str = datafield720Str.replace('COMMITTEE_MEMBER_VALUE', committeeMember)
									newDatafield720Str += '\n'
									parent.addnext(etree.fromstring(newDatafield720Str))
									
						# Remove element if we have no committee members
						else:
							removeNodes.add(parent)

				# Datafield 710, school translation for subfields a and b
				elif parent.attrib['tag'] == '710':

					if child.attrib['code'] == 'a' and child.text == 'SCHOOL_TRANSLATION_SUBFIELDA_VALUE.':
						if marcXmlValues['school'] in schools:
							childText  = child.text.replace('SCHOOL_TRANSLATION_SUBFIELDA_VALUE', schools[marcXmlValues['school']]['subfield_a'])
							child.text = childText
						else:
							notifyJM.log('fail', f"{marcXmlValues['school']} is not defined in school value translation table. Cannot continue writing {xmlRecordFile}.", True)
							return False

					if child.attrib['code'] == 'b':
						if marcXmlValues['school'] in schools:
							if schools[marcXmlValues['school']]['subfield_b']:
								childText  = child.text.replace('SCHOOL_TRANSLATION_SUBFIELDB_VALUE', schools[marcXmlValues['school']]['subfield_b'])
								child.text = childText
							else:
								parent.remove(child)
						else:
							notifyJM.log('fail', f"{marcXmlValues['school']} is not defined in school value translation table. Cannot continue writing {xmlRecordFile}.", True)
							return False
	
				# Datafield 830, degree level tracing (only for certain schools)
				elif parent.attrib['tag'] == '830':
					if child.attrib['code'] == 'p':
						if schools[marcXmlValues['school']]['degree_level_tracing']:
							if 'degreeLevel' in marcXmlValues:
								childText  = child.text.replace('DEGREE_LEVEL_TRACING_VALUE', degreeLevelTracing[marcXmlValues['degreeLevel']])
								child.text = childText
							else:
								notifyJM.log('fail', f"Degree level was not found in mets.xml. Cannot continue writing {xmlRecordFile}.", True)
								return False
								
						# Remove element unless we set degree level tracing for school
						else:
							removeNodes.add(parent)

				# Datafield 852
				elif parent.attrib['tag'] == '852':
					if 'dash_id' in marcXmlValues: # print NET/ETD if there is a dash id
						if child.attrib['code'] == 'b' and child.text == 'NET':
							pass
						elif child.attrib['code'] == 'b' and child.text == 'LIB_CODE_3_CHAR':
							removeNodes.add(parent)
					else: # print LIB_CODE_3_CHAR if there is no dash id
						if child.attrib['code'] == 'b' and child.text == 'LIB_CODE_3_CHAR':
							childText  = child.text.replace('LIB_CODE_3_CHAR', schools[marcXmlValues['school']]['lib_code_3_char'])
							child.text = childText
						elif child.attrib['code'] == 'b' and child.text == 'NET':
							removeNodes.add(parent)

				# Datafield 856, Dash link
				# Remove element if a Dash ID was not found
				elif parent.attrib['tag'] == '856':
					if child.attrib['code'] == 'u':
						if 'dash_id' in marcXmlValues:
							childText  = child.text.replace('DASH_LINK_VALUE', f"{dashLink}{marcXmlValues['dash_id']}")
							child.text = childText
						else:
							removeNodes.add(parent)
	
				# Datafield 506, embargo date
				# Remove element if embargo date is not found
				elif parent.attrib['tag'] == '506':
					if 'dash_id' not in marcXmlValues:
						if 'embargoDate' in marcXmlValues:
							if child.attrib['code'] == 'a' and parent.attrib['ind1'] == '1':
								childText  = child.text.replace('EMBARGO_DATE_VALUE', marcXmlValues['embargoDate'])
								child.text = childText
							else:
								removeNodes.add(parent)
						else: 
							if child.attrib['code'] == 'a' and parent.attrib['ind1'] == '0':
								pass
							else:
								removeNodes.add(parent)
					else:
						removeNodes.add(parent)

				# Datafield 583 field ONLY if there is an 852 LIB_CODE_3_CHAR/GEN
				elif parent.attrib['tag'] == '583':
					if 'dash_id' not in marcXmlValues:
						pass
					else:
						removeNodes.add(parent)

				# Datafield 909, proquest id
				elif parent.attrib['tag'] == '909':
					if child.attrib['code'] == 'k':
						childText  = child.text.replace('LIB_CODE_3_CHAR', schools[marcXmlValues['school']]['lib_code_3_char'])
						child.text = childText
					else:
						removeNodes.add(parent)


	# Remove any node that need to be removed
	if len(removeNodes) > 0:
		for node in removeNodes:
			rootRecord.remove(node)
	
	# Write xml record out in batch directory
	with open(xmlRecordFile, 'w') as xmlRecordOut:
		xmlRecordOut.write('<?xml version="1.0" encoding="UTF-8"?>\n')
		xmlRecordOut.write(f'{xmlStartCollection}\n')
		marcXmlStr = etree.tostring(rootRecord, encoding='unicode')
		xmlRecordOut.write(marcXmlStr)
		xmlRecordOut.write(f'{xmlEndCollection}\n')
		
	notifyJM.log('pass', f'Wrote {xmlRecordFile}', verbose)
	
	# And then return it to be collected with other processed records
	return marcXmlStr


@tracer.start_as_current_span("write_record")
def write_record(proquest_id, school_alma_dropbox, alma_submission_status,
                 insertion_date, last_modified_date,
				 alma_dropbox_submission_date, collection_name, mongo_db):  # pragma: no cover
    logger = logging.getLogger('etd_alma')
    current_span = trace.get_current_span()
    write_success = False

    if mongo_db == None:
        logger.error("Error: mongo db not instantiated")
        current_span.set_status(Status(StatusCode.ERROR))
        current_span.add_event("Error: mongo db not instantiated")		
        return write_success
    try:
        proquest_record = { "proquest_id": proquest_id,
                            "school_alma_dropbox": school_alma_dropbox,
                            "alma_submission_status": alma_submission_status,
                            "insertion_date": insertion_date,
                            "last_modified_date": last_modified_date,
                            "alma_dropbox_submission_date":
							 alma_dropbox_submission_date }
        etds_collection = mongo_db[collection_name]
        etds_collection.insert_one(proquest_record)
        logger.info("proquest id " + str(proquest_id) + " written to mongo")
        current_span.add_event("proquest id " + str(proquest_id) + " written to mongo")
        write_success = True
    except Exception as err:
        current_span.set_status(Status(StatusCode.ERROR))
        logger.error("Error: unable to connect to mongodb", exc_info=True)
        current_span.add_event("Error: unable to connect to mongodb")
    return write_success

def escapeStr(s):
	# 1) remove multiple repeated spaces
    s = re.sub(r"\s+"," ", s)
	# 2) replace smart quotes
    s = s.replace('“', '"').replace('”', '"')
	# 3) escape xml characters
    s = escape(s)
    return s
