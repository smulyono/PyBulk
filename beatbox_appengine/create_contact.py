# create_contact.py

import os
import beatbox

from google.appengine.ext.webapp import template
from google.appengine.ext.webapp import RequestHandler

class CreateContactHandler(RequestHandler):
    def get(self):
        self.redirect('/static/create_contact_input.html')
        
    def post(self):
        # Retrieve username, password and account from post data
        username = self.request.get('uid')
        password = self.request.get('pwd')
        firstName = self.request.get('firstName')
        lastName = self.request.get('lastName')
        phone = self.request.get('phone')
        email = self.request.get('email')
        
        # Attempt a login
        self.sforce = beatbox.PythonClient()
        try:
            login_result = self.sforce.login(username, password)
        except beatbox.SoapFaultError, errorInfo:
            path = os.path.join(os.path.dirname(__file__), 'templates/simple_login_failed.html')
            self.response.out.write(template.render(path, {'errorCode': errorInfo.faultCode, 
                                                           'errorString': errorInfo.faultString}))
            return
        
        # Create the new contact  
        newContact = {'FirstName': firstName, 'LastName': lastName, 'Phone': phone, 'Email': email,
                      'LeadSource': 'Google AppEngine', 'type': 'Contact'}
        create_result = self.sforce.create([newContact])
        
        # Render the output
        template_values = {'success': create_result[0]['success'], 'objId': create_result[0]['id'], 
                           'errors': create_result[0]['errors']}
        path = os.path.join(os.path.dirname(__file__), 'templates/create_contact_result.html')
        self.response.out.write(template.render(path, template_values))
        