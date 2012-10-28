'''
Created on Oct 27, 2012

Command line application to test the pybulk library.
Operations tested are CRUD operation 

How to execute:
$ python crudrest.py

@author: sannymulyono
'''
from pybulk import sfBulk, loadFromCSVFile, sfBulkJobInfo
import os
import sys
from pprint import pprint
from time import sleep

# added pybulk library 
beatbox_path = os.path.abspath("pybulk")
if beatbox_path not in sys.path:
    sys.path.insert(0,beatbox_path)

if __name__ == '__main__':
    # for bulk operation
    org_bulk = raw_input("Specify your instance org (e.g https://na1.salesforce.com) : ")
    print """
Choose your instance mode:
    1. Production / Development
    2. Sandbox
    """

    org_choice = None
    while org_choice != "1" and org_choice != "2":
        org_choice = raw_input(">> ")
    
    # initate login
    obb = sfBulk(org_bulk);
    # for sandbox login operation
    if (org_choice == "2"):
        obb.sandboxmode = True
    
    username = raw_input("Specify username : ")
    password = raw_input("Specify password : ")
    
    if username == "" :
        username = None
    if password == "" : 
        password = None
        
    obb.login(username, password)
    print "User logged in ! \n"
    
    
    input = None
    while input != "99":
        print """
    Operation:
    1. Create , Update, Delete operation from CSV File
    2. Query from CSV File
    3. Query (SOQL input)
    99. I am done
        """
        input = raw_input(">> ")
        
        if input == "1":
            # Create new job
            jobinfo = sfBulkJobInfo()
            
            operation_choice = raw_input("Operation (C)reate (U)pdate (D)elete : ")
            if operation_choice.lower() == "c":
                jobinfo.operation = "Insert"
            
            if operation_choice.lower() == "u":
                jobinfo.operation = "Update"

            if operation_choice.lower() == "d":
                jobinfo.operation = "delete"
                
            jobinfo._object = "Account"
            jobinfo.contentType = "CSV"
            jobinfo.concurrencyMode = "Parallel"
        
            obb.createJob(jobinfo)
            
            # Create new Batch
            csv_filename = raw_input("CSV File name to load : " )
            max_csvrecord = raw_input("Max record for each batch (default : " + str(obb.MAX_RECORDS) + ") ")
            batchIds = obb.createBatchFromCSV(jobinfo, csv_filename, max_csvrecord)
            
            pprint("Total Batch : " + str(len(jobinfo.batch)))
            # wait until it finishes
            while (not obb.is_jobs_completed(jobinfo)):
                sleep(1)
            
            pprint("Last status of batch : \n" )
            pprint(jobinfo.batch)
            pprint("\n")   
            # close the job
            obb.closeJob(jobinfo)
            
        if input == "2":
            # Create new job
            jobinfo = sfBulkJobInfo()
            jobinfo.operation = "query"
            jobinfo._object = "Account"
            jobinfo.contentType = "CSV"
            jobinfo.concurrencyMode = "Parallel"
        
            obb.createJob(jobinfo)
            
            # Create new Batch
            csv_filename = raw_input("CSV File name to load : " )
            # For this single line query, we will use the mode of omit_header in loadFromCSVFile
            batchId = obb.createBatch(jobinfo, loadFromCSVFile(csv_filename, 10000, True)[0])
            
            ## Uncomment this for manual soql input
            ## soql_input = raw_input("SOQL : ")
            ##batchId = obb.createBatch(jobinfo, soql_input)
            
            pprint("Total Batch : " + str(len(jobinfo.batch)))
            # wait until it finishes
            while (not obb.is_jobs_completed(jobinfo)):
                sleep(1)
            
            pprint("Last status of batch : \n" )
            pprint(jobinfo.batch)
            pprint("\n")   
            
            # show the result to console
            pprint("Receiving Result from Salesforce : \n")
            pprint(obb.showBatchResult(jobinfo, batchId))
            
            # close the job
            obb.closeJob(jobinfo)
        
        if input == "3":
            # Create new job
            jobinfo = sfBulkJobInfo()
            jobinfo.operation = "query"
            jobinfo._object = "Account"
            jobinfo.contentType = "CSV"
            jobinfo.concurrencyMode = "Parallel"
        
            obb.createJob(jobinfo)
            
            # Create new Batch
            soql_input = raw_input("SOQL : ")
            batchId = obb.createBatch(jobinfo, soql_input)
            
            pprint("Total Batch : " + str(len(jobinfo.batch)))
            # wait until it finishes
            while (not obb.is_jobs_completed(jobinfo)):
                sleep(1)
            
            pprint("Last status of batch : \n" )
            pprint(jobinfo.batch)
            pprint("\n")   
            
            # show the result to console
            pprint("Receiving Result from Salesforce : \n")
            pprint(obb.showBatchResult(jobinfo, batchId))
            
            # close the job
            obb.closeJob(jobinfo)            
    # logout completely
    obb.logout()
        
    