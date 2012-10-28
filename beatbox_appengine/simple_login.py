# simple_login.py

import os
import beatbox

from google.appengine.ext.webapp import template
from google.appengine.ext.webapp import RequestHandler

class SimpleLoginHandler(RequestHandler):
	def post(self):
		# Retrieve username and password from post data
		username = self.request.get('uid')
		password = self.request.get('pwd')
		
		# Attempt a login
		self.sforce = beatbox.PythonClient()
		try:
			login_result = self.sforce.login(username, password)
		except beatbox.SoapFaultError, errorInfo:
			path = os.path.join(os.path.dirname(__file__), 'templates/simple_login_failed.html')
			self.response.out.write(template.render(path, {'errorCode': errorInfo.faultCode, 
			                                               'errorString': errorInfo.faultString}))
		else:
			# Grab the resulting session id
			template_values = {'user_id': login_result['userId'],
			                   'server_url': login_result['serverUrl'],
			                   'session_id': login_result['sessionId']};
		
			# Render the output
			path = os.path.join(os.path.dirname(__file__), 'templates/simple_login_result.html')
			self.response.out.write(template.render(path, template_values))
		