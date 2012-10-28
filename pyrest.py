'''
Created on Oct 26, 2012

@author: sannymulyono
'''

import sys
import os
from pprint import pprint
from time import sleep

# added pybulk library 
beatbox_path = os.path.abspath("pybulk")
if beatbox_path not in sys.path:
    sys.path.insert(0,beatbox_path)
from pybulk import sfBulk, loadFromCSVFile
from pybulk import sfBulkJobInfo

if __name__ == '__main__':
    # initate login
    obb = sfBulk();
    obb.login("atroy@smulyono.com", "troy1234yQS8TgQ9oGKdeFGrf104LnTe")
    
    # Create new job
    jobinfo = sfBulkJobInfo()
    jobinfo.operation = "query"
    jobinfo._object = "Account"
    jobinfo.contentType = "CSV"
    jobinfo.concurrencyMode = "Parallel"

    obb.createJob(jobinfo)
    # Create new Batch
    batchIds = obb.createBatch(jobinfo, loadFromCSVFile("query_csv.txt", 100000, True)[0])
    
    # wait until it finishes
    while (not obb.is_jobs_completed(jobinfo)):
        sleep(1)
    
    obb.showBatchResult(jobinfo, batchIds)
        
    # close the job
    obb.closeJob(jobinfo)
    
    # logout
    obb.logout()
        