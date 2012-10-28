# acct_lookup.py

import os
import beatbox

from google.appengine.ext.webapp import template
from google.appengine.ext.webapp import RequestHandler

class AcctLookupHandler(RequestHandler):
	def get(self):
		path = os.path.join(os.path.dirname(__file__), 'templates/acct_lookup.html')
		self.response.out.write(template.render(path, {'accounts': []}))
	def post(self):
		# Retrieve username, password and account from post data
		username = self.request.get('uid')
		password = self.request.get('pwd')
		accountName = self.request.get('account')
		
		# Attempt a login
		self.sforce = beatbox.PythonClient()
		try:
			login_result = self.sforce.login(username, password)
		except beatbox.SoapFaultError, errorInfo:
			path = os.path.join(os.path.dirname(__file__), 'templates/simple_login_failed.html')
			self.response.out.write(template.render(path, {'errorCode': errorInfo.faultCode, 
			                                               'errorString': errorInfo.faultString}))
			return
				
		# Query for accounts	
		query_result = self.sforce.query("SELECT Id, Name, Phone, WebSite FROM Account WHERE Name LIKE '%" + 
										 accountName + "%'")
		records = query_result['records']
		
		# Render the output
		template_values = {'accounts': records,
						   'username': username,
						   'password': password,
						   'accountName': accountName};
		path = os.path.join(os.path.dirname(__file__), 'templates/acct_lookup.html')
		self.response.out.write(template.render(path, template_values))
		