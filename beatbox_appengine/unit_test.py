# unit_test.py

import os
import beatbox

from datetime import datetime

import logging
from google.appengine.api import memcache
from google.appengine.ext.webapp import template
from google.appengine.ext.webapp import RequestHandler

class UnitTestHandler(RequestHandler):
	def get(self):
		op = self.request.get('op')
		sid = self.request.get('sid')
		logging.info( sid)
		
		if (sid == '' or sid == None):
			self.redirect('/static/unit_test_login.html')
			return 
		
		if ( op == '' ):
			
			logging.info(memcache.get('userInfo'))
			unifo = memcache.get('userInfo')
			template_values = {'user_id': unifo.get('userId'),
			               'server_url':  memcache.get("serverUrl") ,
			               'session_id': sid};
			path = os.path.join(os.path.dirname(__file__), 'templates/unit_test.html')
			self.response.out.write(template.render(path, template_values))
			return
		
		client = Client()
		
		if (op == 'query'): 
			soql = 'select name from Account limit 12'
			self.response.out.write('<b>'+soql+'</b>')   
			
			query_result = client.query( soql )
			for account in query_result['records'] : 	
				self.response.out.write('<li>' + account['Name'] )   
			
			
		if (op == 'login'):
			login_result = client.login( memcache.get('username'),
										 memcache.get('password') )
			self.response.out.write( login_result )
			
			
		if (op == 'create'):
			sobjects = []
			new_acc = { 'type': 'Account', 'name' : 'new GAE account' }
			sobjects.append(new_acc)
			results = client.create(sobjects)
			self.response.out.write( results )
			
			
		if (op == 'delete'):
			query_result = client.query( "select id from Account where name like 'new GAE account%' limit 1")
			accounts = []
			for account in query_result['records'] :
				accounts.append( account['Id'] )
			
			if ( accounts.__len__() < 1 ): 
				self.response.out.write('no account found with name : new GAE account ')
			else 	:
				results = client.delete( accounts )
				self.response.out.write( results )
			
			
		if ( op == 'update'):
			query_result = client.query( "select id from Account where name like 'new GAE account%' limit 1")
			
			if ( query_result['records'].__len__() < 1 ): 
				self.response.out.write('no account found with name : new GAE account ')
			else 	:
				account = query_result['records'][0]
				self.response.out.write( account )
				
				account['Name'] = 'new GAE account UPDATED'
				
				results = client.update( account )
				self.response.out.write( results )
		
		if (op == 'global' ):
			describe = client.describeGlobal()
			self.response.out.write( describe )
			
		if (op == 'describeAcc'):
			# describeSObjects returns a list of dictionaries
			dict = client.describeSObjects('Account')[0]
			self.response.out.write( '<dl>')
			for key in dict.keys():
				self.response.out.write( '<dt>'+ key + '</dt><dd>' 
										 + str(dict[key]) + '</dd>' )
				
				
		if ( op == 'retrieve' ):
			query_result = client.query( "select id from Account where name like 'new GAE account%' limit 1")
			
			if ( query_result['records'].__len__() < 1 ): 
				self.response.out.write('no account found with name : new GAE account ')
			else:
				accounts = []
				for account in query_result['records'] :
					accounts.append( account['Id'] )
				results = client.retrieve( 'Name,Id','Account', accounts )
				self.response.out.write( results )
				
		if ( op == 'getDeleted' ): 
			now = datetime.now()
			then = datetime(now.year, now.month, now.day-1 )
			results = client.getDeleted('Account', then, now )
			self.response.out.write( results )

		if ( op == 'getUpdated' ): 
			now = datetime.now()
			then = datetime(now.year, now.month, now.day-1 )
			results = client.getUpdated('Account', then, now )
			self.response.out.write( results )
		
		if ( op == 'getUserInfo' ):
			results = client.getUserInfo( )
			self.response.out.write( results )
		
		
		if (op == 'describeTabs' ):
			pass
			
		# add a back link
		self.response.out.write('<p /><b><a href="/unittest?sid='+memcache.get("sessionId")+'">back</a></b>' )
		
	def post(self):
		# Retrieve username and password from post data
		login_result = verifyLogin(self)
		if ( login_result == None ):
			# render a failed login
			path = os.path.join(os.path.dirname(__file__), 'templates/test_login_failed.html')
			self.response.out.write(template.render(path, 
				{'errorCode': memcache.get('errorCode'), 
			     'errorString': memcache.get('errorString') }))
			return
		
		# login was ok
		template_values = {'user_id': login_result['userId'],
			               'server_url': login_result['serverUrl'],
			               'session_id': login_result['sessionId']};
		# Render the output
		path = os.path.join(os.path.dirname(__file__), 'templates/unit_test.html')
		self.response.out.write(template.render(path, template_values))
		
		
def Client():
    bbox = beatbox.PythonClient()
    bbox.useSession(memcache.get('sessionId'), memcache.get('serverUrl'))
    return bbox
   		
def verifyLogin(self):
    """
    Insure that we have a session id or have a login username and can use it
    store our session info in memcache
    """
    username = self.request.get('uid')
    password = self.request.get('pwd')
    memcache.set_multi({ 'username':username, 'password':password})
    self.sforce = beatbox.PythonClient()
    login_result = None
    try:
    	
	    login_result = self.sforce.login(username, password)
	    
    except beatbox.SoapFaultError, errorInfo:
		memcache.set_multi( {'errorCode': errorInfo.faultCode, 
                             'errorString': errorInfo.faultString})
		
    else:
    	# caution: this method of storing session id is not viable for multiple users
    	# of this application since all users will have access to the same session id and concurrent
    	# users will overwrite this info. this method will work for a single user at a time 
        memcache.set_multi({ 
						   	'sessionId': login_result['sessionId'], 
			                'serverUrl': login_result['serverUrl'],
			                'metadataServerUrl' : login_result['metadataServerUrl'],
			                'userInfo': login_result['userInfo']} , time=36000)       
  
    logging.info( memcache.get("sessionId") )
    logging.info( memcache.get("serverUrl") )
    logging.info( memcache.get("metadataServerUrl") )
    logging.info( memcache.get("userInfo") )
    
    return login_result
	