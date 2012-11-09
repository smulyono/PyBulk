Salesforce Bulk API Demo with Python
====================================

Cloudspokes Challenge : 
-----------------------
http://www.cloudspokes.com/challenges/1857

Requirement:
------------
The application will demonstrate how to create, read, update and delete records in Salesforce.com in bulk using the Bulk API. 

###For login and logout operation, utilizing the Beatbox library from 
<https://github.com/superfell/Beatbox>

###How to Run :
    $ python crudtest.py

###Video Instruction : 
<http://www.screencast.com/t/00cYnKLB> -- showing how to use CUD operation

<http://www.screencast.com/t/oJxfheP9dv6> -- show how to do bulk query


Instructions to do CRUD Operation
=================================

##(C)reate Operation:
``` python
    # initate login
    obb = sfBulk();
    obb.login(username, password)
    # Create new job
    jobinfo = sfBulkJobInfo()
    # set up job operation
    jobinfo.operation = "insert"
    # set up object to operate
    jobinfo._object = "Account"
    # set up the content type for the batch data, For this version only CSV Is supported
    jobinfo.contentType = "CSV"
    # set up concurrencyMode (optional)
    jobinfo.concurrencyMode = "Parallel"
    # the actual creation of the job
    obb.createJob(jobinfo)
    # Create new Batch
    batchIds = obb.createBatchFromCSV(jobinfo, "insert_csv.txt")
    # close the job
    obb.closeJob(jobinfo)
    # logout
    obb.logout()
```

##(U)pdate Operation:
```python
    obb = sfBulk();
    obb.login(username, password)
    jobinfo = sfBulkJobInfo()
    jobinfo.operation = "update"
    jobinfo._object = "Account"
    jobinfo.contentType = "CSV"
    obb.createJob(jobinfo)
    batchIds = obb.createBatchFromCSV(jobinfo, "update_csv.txt")
    obb.closeJob(jobinfo)
    obb.logout()
```

##(D)elete Operation:
```python
    obb = sfBulk();
    obb.login(username, password)
    jobinfo = sfBulkJobInfo()
    jobinfo.operation = "delete"
    jobinfo._object = "Account"
    jobinfo.contentType = "CSV"
    obb.createJob(jobinfo)
    batchIds = obb.createBatchFromCSV(jobinfo, "delete_csv.txt")
    obb.closeJob(jobinfo)
    obb.logout()
```

##(R)ead / Query Operation:
```python
    obb = sfBulk();
    obb.login(username, password)
    jobinfo = sfBulkJobInfo()
    jobinfo.operation = "query"
    jobinfo._object = "Account"
    jobinfo.contentType = "CSV"
    obb.createJob(jobinfo)
    batchIds = obb.createBatchFromCSV(jobinfo, "query_csv.txt")
    # you can also use SOQL directly using
    # batchId  = obb.createBatch(jobInfo, "Select id, name from Account")
    obb.closeJob(jobinfo)
    obb.logout()
```

##Another operation included in the library:
    * Ability to find batch status, Example during query
```python 
    obb = sfBulk();
    obb.login(username, password)
    jobinfo = sfBulkJobInfo()
    jobinfo.operation = "query"
    jobinfo._object = "Account"
    jobinfo.contentType = "CSV"
    obb.createJob(jobinfo)
    batchIds = obb.createBatchFromCSV(jobinfo, "query_csv.txt")
    # wait until it finishes
    while (not obb.is_jobs_completed(jobinfo)):
        pprint(jobinfo.batch)
        sleep(1)
    
    # show the result to console
    pprint("Receiving Result from Salesforce : \n")
    # Individually look for batch result
    pprint(obb.showBatchResult(jobinfo, batchIds[0]))
    
    obb.closeJob(jobinfo)
    obb.logout()
```   
    