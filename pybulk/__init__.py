'''

Bulk API implementation 
For login and logout operation, utilizing the Beatbox library from https://github.com/superfell/Beatbox

There are 3 objects included in this file
* Callout       --> helper class to initiate GET / POST HTTP methods 
* sfBulkJobInfo --> class to holds the Job information. Each jobs can contains multiple batch
                    Some important methods : 
                        ** createJob --> to create individual job 
                        ** closeJob  --> close the job
* sfBulk        --> main class to operate the bulk API
                    Some important methods:
                        ** createBatchFrom CSV --> will accepts CSV file as input and divide those into batches based on MAX_RECORDS
                        ** updateBatchStatus   --> shows individual batch information 

Created on Oct 26, 2012
@author: sannymulyono
'''
from pprint import pprint
from xml.dom.minidom import *
import httplib
import inspect
import os
import sys
import urllib
import urllib2

# added beatbox library 
beatbox_path = os.path.realpath(os.path.abspath( \
                                os.path.join( \
                                   os.path.split(inspect.getfile(inspect.currentframe()))[0],"beatbox") \
                                ))
if beatbox_path not in sys.path:
    sys.path.insert(0,beatbox_path)
import beatbox
from beatbox import SoapFaultError
import xmltramp

class Callout(object):
    """
    Helper class to easily do GET / POST method along with the encoded data
    """
    headerAuth = None
    sessionid = None
    
    
    def __init__(self, header = {}):
        if 'Authorization' in header :
            self.headerAuth=header['Authorization']
    
    # initiate http call (get / post) along with the data    
    # @param String url      , server url information for the callout destination
    # @param String method   , "GET" or "POST"
    # @param String tdata    , data information which will be submitted on the BODY
    # @param dict   headers  , header information
    def docall(self,url , method ,tdata = None, headers = None):
        handler = urllib2.HTTPHandler()
        opener = urllib2.build_opener(handler)
        if tdata is not None:
            if type(tdata) == dict :
                data = urllib.urlencode(tdata)
            else :
                data = tdata
        else:
            data = urllib.urlencode({})
        request = urllib2.Request(url, data)
        
        if headers is not None:
            for keyh, valueh in headers.iteritems():
                request.add_header(keyh, valueh)
        request.get_method = lambda : method
        try:
            connection = opener.open(request)
        except urllib2.HTTPError,e:
            connection = e
            pprint(str(e.read()))
        
        return connection.read()

class sfBulkJobInfo(object):
    """
    Class to hold jobs information. Every jobs information can holds multiple batches; 
    each of the batch id are stored in property "batch"
    
    """
    # properties 
    id = None
    state = None
    debug_result = None
    operation = None
    _object = None
    externalfieldname = None
    jobstate = None
    concurrencyMode = None
    contentType = None
    
    batch = None
    
    def __init__(self):
        self.batch = {}
    
    # Lookup latest batch state which is stored
    # @param String batchId     , batch id which is inspected for it's batch status
    def findBatchState(self, batchId):
        if batchId in self.batch:
            return self.batch[batchId]
        else:
            return None
        
    # Creating individual job information. This information will correspondent to single job information in Salesforce
    def createJob(self):
        # header
        root = Document()
        jobinfo = root.createElement("jobInfo");
        jobinfo.setAttribute("xmlns", "http://www.force.com/2009/06/asyncapi/dataload")
        
        # iterate the property ( order is important )
        if self.operation is not None:
            # operation must be in lower case
            jobinfo.appendChild(createxmlNode("operation", str(self.operation).lower()))
        if self._object is not None:
            jobinfo.appendChild(createxmlNode("object", self._object))
        if self.concurrencyMode is not None:
            jobinfo.appendChild(createxmlNode("concurrencyMode", self.concurrencyMode))
        if self.externalfieldname is not None:
            jobinfo.appendChid(createxmlNode("externalfieldname", self.externalfieldname))
        if self.contentType is not None:
            jobinfo.appendChild(createxmlNode("contentType", self.contentType))
        
        # finally put them all to root document
        root.appendChild(jobinfo)
        return root.toxml("utf-8")
    
    # Close the individual job information
    def closeJob(self):
        # header
        root = Document()
        jobinfo = root.createElement("jobInfo");
        jobinfo.setAttribute("xmlns", "http://www.force.com/2009/06/asyncapi/dataload")
        
        # iterate the property ( order is important )
        if self.state is not None:
            # operation must be in lower case
            jobinfo.appendChild(createxmlNode("state", self.state))
        
        # finally put them all to root document
        root.appendChild(jobinfo)
        return root.toxml("utf-8")
    
class sfBulk(object):
    """
    Salesforce Bulk API Implementation
    
    Main class to intiate bulk operation.
    """
    # constants
    CONTENT_TYPE_XML = "application/xml"
    CONTENT_TYPE_CSV = "text/csv"
    REQUEST = "request"
    RESULT  = "result"
    BATCH   = "batch"
    JOB     = "job"
    USERAGENT= "Python-BulkApiClient/26.0.0"
    API_VERSION= "26.0"
    MAX_RECORDS = 10000
    
    # LOGIN CREDENTIALS , CHANGE THIS FOR YOUR ORGANIZATION
    USERNAME = "PUT YOUR USERNAME HERE"
    PASSWORD = "PUT YOUR PASSWORD AND SECURITY TOKEN"
    
    # CONSTANTS FOR XML ELEMENT IDENTIFIER
    ELEMENT_NODE = 1
    TEXT_NODE = 3
    
    # standard constructor
    # @param String url_instance, url to connect for the bulk operation
    # @param String newsession,   sessionid which can be use for initial. This property will ALWAYS be overriden
    #                      when you use the login method
    def __init__(self, url_instance="https://na9.my.salesforce.com", newsession = None):
        beatbox.gzipRequest = False
        self.sf = beatbox._tPartnerNS
        self.svc = beatbox.Client()
        self.bulk_server = url_instance
        self.sessionid = newsession
        self.callClient = None
        self.sandboxmode = False
    
    # initiate login to get the session id. Utilize beatbox library
    # @param String username (optional), other than using constant USERNAME; the library can also accept username as parameter
    # @param String password (optional), other than using constant PASSWORD; the library can also accept username as parameter     
    def login (self, username=None, password=None):
        # override beatbox server url
        if self.sandboxmode :
            self.svc.serverUrl = "https://test.salesforce.com/services/Soap/u/" + self.API_VERSION
        else:
            self.svc.serverUrl = "https://login.salesforce.com/services/Soap/u/" + self.API_VERSION

        if username is None:
            username = self.USERNAME
        if password is None:
            password = self.PASSWORD
        
        try :
            loginResult = self.svc.login(username,password)
            # update the session
            self.sessionid = str(loginResult[self.sf.sessionId])
        except SoapFaultError, e:
            sys.exit("unable to continue! -- " + e.faultString)
            
    # logout user completely 
    def logout(self):
        if self.sessionid is not None:
            self.svc.logout()
            self.sessionid = None
    
    # creating new job
    # @param sfBulkJobInfo jobinfo, this parameter will be used to populate the job information 
    def createJob(self, jobinfo):
        resp = self.bulkHttp(self.JOB, 
                                jobinfo.createJob(), 
                                {"Content-Type" : self.CONTENT_TYPE_XML + '; charset=UTF-8'})
        # create dict for easier manipulation
        dict_result = self.parseXMLResult(resp)
        if "id" in dict_result:
            self.runningJobId = dict_result["id"]
            jobinfo.id = dict_result["id"]
            jobinfo.debug_result = dict_result
            print "Job "+ jobinfo.id + " created ..... \n"
    
    # closing job 
    # @param sfBulkJobInfo jobinfo, indicate job information which needs to be closed 
    def closeJob(self, jobinfo):
        jobinfo.state="Closed"
        resp = self.bulkHttp(self.JOB + "/" + jobinfo.id, 
                                jobinfo.closeJob(), 
                                {"Content-Type" : self.CONTENT_TYPE_XML + '; charset=UTF-8'})
        # create dict for easier manipulation
        dict_result = self.parseXMLResult(resp)
        if "id" in dict_result:
            self.runningJobId = dict_result["id"]
            jobinfo.id = dict_result["id"]
            jobinfo.debug_result = dict_result
            jobinfo.state = dict_result["state"]
            print "Job "+ jobinfo.id + " " + jobinfo.state + "..... \n"
    
    # create individual batch operation with batchdata (as string) 
    # @param sfBulkJobInfo jobinfo, job information
    # @param String batchdata     , information which will be sent (e.g SOQL, CSV lines in string)
    def createBatch(self, jobinfo, batchdata):
        if self.runningJobId is None:
            sys.stderr.write("No running job id is found, inituate runJob first!\n")
            return False
        # only CSV is supported
        resp = self.bulkHttp(self.JOB + "/" + self.runningJobId + "/" + self.BATCH, 
                             batchdata, 
                             {"Content-Type" : self.CONTENT_TYPE_CSV + '; charset=UTF-8'})
        # create dict for easier manipulation
        dict_result = self.parseXMLResult(resp)
        if "id" in dict_result:
            jobinfo.batch[dict_result["id"]] = dict_result["state"]
            print "Batch " + dict_result["id"] + " status is " + dict_result["state"] + "\n"
            return dict_result["id"]
        else:
            return ""
    
    # create batch from csv file, also includes the max_record limitation
    # @param sfBulkJobInfo jobinfo       , job information
    # @param String csvfile              , CSV file name to load and will be divided based on the max_record limitation
    def createBatchFromCSV(self, jobinfo, cvsfile, maxrecord = None):
        batches_id = []
        if maxrecord is None or \
            maxrecord == ''  :
            maxrecord = self.MAX_RECORDS
        batches_file = loadFromCSVFile(cvsfile, int(maxrecord))
        for batch_file in batches_file :
            batchid = self.createBatch(jobinfo, batch_file)
            batches_id.append(batchid)
        return batches_id

    # update individual batch status    
    # @param sfBulkJobInfo jobinfo       , job information
    # @param String batchId              , batch id 
    def updateBatchStatus(self, jobinfo, batchId):
        # only CSV is supported
        resp = self.bulkHttp(self.JOB + "/" + self.runningJobId + "/" + self.BATCH + "/" + batchId, 
                                None, 
                                {"Content-Type" : self.CONTENT_TYPE_CSV + '; charset=UTF-8'},
                                "GET")
        # create dict for easier manipulation
        dict_result = self.parseXMLResult(resp)
        if "id" in dict_result:
            if dict_result["id"] in jobinfo.batch:
                jobinfo.batch[dict_result["id"]] = dict_result["state"]
                print "Batch " + dict_result["id"] + " ... " + dict_result["state"]
    
    # show the specific batch result
    # @param sfBulkJobInfo jobinfo       , job information
    # @param String batchId              , batch id 
    def showBatchResult(self, jobinfo, batchId):
        # only CSV is supported
        resp = self.bulkHttp(self.JOB + "/" + self.runningJobId + "/" + self.BATCH + "/" + batchId + "/" + self.RESULT, 
                                None, 
                                {"Content-Type" : self.CONTENT_TYPE_CSV + '; charset=UTF-8'},
                                "GET")
        # create dict for easier manipulation
        try :
            dict_result = self.parseXMLResult(resp)
            if "result" in dict_result:
                resp = self.bulkHttp(self.JOB + "/" + self.runningJobId + "/" + self.BATCH + "/" + batchId + "/" + self.RESULT+ "/" + dict_result["result"], 
                                        None, 
                                        {"Content-Type" : self.CONTENT_TYPE_CSV + '; charset=UTF-8'},
                                        "GET")
                pprint(resp)
        except Exception, e:
            pprint (e)
    
    # helper methods to transform XML to dict
    # @param String raw_xml      , XML which is represented in string
    def parseXMLResult(self, raw_xml):
        # parse the job result
        retval = {}
        
        parse_resp = parseString(raw_xml)
        Root = parse_resp.documentElement
        
        for child in Root.childNodes:
            if child.nodeType == self.ELEMENT_NODE :
                retval = self._parseElement(child.childNodes, retval)
        return retval
    
    # helper methods to parse each XML Element
    # @param XMLElement nodeElement      
    # @param dict   dataval         
    def _parseElement(self, nodeElement, dataval):
        if type(nodeElement) == NodeList:
            for child in nodeElement :
                self._parseElement(child, dataval)
            return dataval
            
        if nodeElement.nodeType == self.TEXT_NODE : 
            dataval[nodeElement.parentNode.nodeName] = nodeElement.nodeValue
            return dataval
        else :
            if nodeElement.nodeType == self.ELEMENT_NODE:
                self._parseElement(nodeElement.childNodes, dataval)
    
    # methods to run http callout to salesforce
    # @param String bulkmethod         , what kind of bulk request (e.g self.JOB, self.BATCH)
    # @param String submitdata         , data to be submitted
    # @param dict   pheaders           , HTTP header information
    # @param String httpmethods        , GET / POST methods to be used in bulk request
    def bulkHttp(self, bulkmethod, submitdata = None, pheaders = None, httpmethods="POST"):
        headers = self.__standardHeaders()
        # add additional headers or override
        if headers is not None :
            if type(pheaders) == dict :
                for keyh, valueh in pheaders.iteritems():
                    headers[keyh] = valueh
            
        if self.callClient is None :
            if self.sessionid is not None :
                self.callClient = Callout()
            else:
                return False
        resp = self.callClient.docall( self.__constructBulkUrl(bulkmethod) , httpmethods, submitdata, headers)
        return resp
    
    # will check whether all batches in specific jobinfo record is completed
    # @param sfBulkJobInfo jobinfo    , job information
    def is_jobs_completed(self, jobinfo):
        completed = True
        for batchId in jobinfo.batch:
            self.updateBatchStatus(jobinfo, batchId)
            ## put the condition for completed jobs criteria
            if jobinfo.findBatchState(batchId) != "Completed":
                completed = False
        return completed
    
    # helper method to create valid bulk operation url
    # @param String bulkmethod  , what kind of bulk request (e.g self.JOB, self.BATCH)
    def __constructBulkUrl(self, bulkmethod):
        return self.bulk_server + '/services/async/' + self.API_VERSION + "/" + bulkmethod
    
    # prepare standard headers information
    def __standardHeaders(self):
        headersValue = {
                        "X-SFDC-Session" : self.sessionid,
                        "Accept" : "application/xml",
                        "User-Agent" : self.USERAGENT
                        }
        return headersValue
    
# utility to create XML Node Element
# @param string element, XML Node tag 
# @param string value  , XML node tag value
def createxmlNode(element, value):
    xmls = Element(element)
    xmls.appendChild(Document().createTextNode(value))
    return xmls

# simple utility to load from csv file
# Assumption : first line will be the field name
def loadFromCSVFile(filename, max_count=10000, omit_header=False):
    sane_files = []
    retval= ""
    linecount = 0
    headerLines = ""
    for line in open(filename):
        retval += line
        linecount += 1
        if headerLines == "" and not omit_header:
            headerLines = line
            linecount = 0
        if linecount >= max_count:
            linecount = 0 
            sane_files.append(retval)
            retval = ""
            retval += headerLines
            
    if retval != "" and retval != headerLines:
        sane_files.append(retval)
    return sane_files