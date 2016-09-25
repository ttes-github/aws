from __future__ import print_function

# !/usr/bin/env python
# -*-coding: utf-8 -*-
# cording:utf-8
# This code copies data in uploaded csv files to s3 bucket


import boto3  # aws lambda python module (python2.7)
import json
import csv  # csv module
import os
import re
import linecache


print('Loading function')

# aws s3 information for connection-----------------------\
s3_client = boto3.client('s3')  # .client or .resource

## Event handler------------------------------------------
def lambda_handler(event, context):
    # Get uploded file names and paths
    bucket = event['Records'][0]['s3']['bucket']['name']  # Get the latest event record of s3 bucket
    key = event['Records'][0]['s3']['object']['key']  # Get the latest put filename
    path_tmp = u'/tmp/' + os.path.basename(key)  # path to the uploded file (filename)
    path_copy = re.sub(r'^original_csv/', u'read_csv/', key)  # replace the file path to place for coping csv file

    try:
        # Download the file from s3 original folder to temp
        s3_client.download_file(Bucket=bucket, Key=key, Filename=path_tmp)
        # read csv file and return data array
        loadData = csvdata_to_db(path_tmp)  # Read data array from uploaded csv file
        nrows = len(loadData); # number of data
        ncols = len(loadData[0]); # number of data channels

        # Save the file to s3 read folder
        s3_client.upload_file(Filename=path_tmp, Bucket=bucket, Key=path_copy)

        # Connection to dynamo db
        dynamodb_client = boto3.resource('dynamodb')
        table_name = 'dynamodb_table'  # define name
        table = dynamodb_client.Table(table_name)

        # Create a record
        with table.batch_writer() as batch:
            for ii in range(nrows - 1):
                batch.put_item(
                    Item={
                        'username': 'hoge',  # Primary key
                        'filename': os.path.basename(key),
                        'sensor_id': 1,
                        'id': ii,
                        'data_id': ii + 1,
                        'timestamp': loadData[ii][0],
                        'ch1': loadData[ii][0 + 2]
                    }
                )

    except Exception as e:
        print(e)
        # raise e


## Reading data array from csv file ---------------------------
def csvdata_to_db(filename):
    # filename = 'readdata.csv'
    readfile = open(filename, 'r')  # open readfile
    reader = csv.reader(open(filename, 'r'))  # create reader object
    nlines = len(readfile.readlines())  # get number of lines in readfile
    startIndex = 71  # data start from this index value in csv file (ignore header)
    endIndex = nlines - 8  # Remove Footer (NR-600 format)
    nData = endIndex - startIndex + 1
    nCH = len(linecache.getline(filename, startIndex).split(","))  ## Get number of data channels and remove time stamps
    # print linecache.getline(filename, startIndex)
    # print nCH, nlines, startIndex


    ## Create Data Array to send to RDS database
    loadData = [[0 for i in range(nCH - 1)] for j in range(nData)]  # Allocate saveData array
    with open(filename, "r") as f:  # no need to call close file
        for ii in xrange(nlines):
            readData = f.readline().rstrip('\n')  # read lines one by one and delete /n
            split_readData = readData.split(",")
            # print split_readData, ii
            if (ii >= startIndex and ii <= endIndex):  # ignore header and footer
                # print('in loop'[3:])
                # print split_readData[2:]
                loadData[ii - startIndex][0:nCH + 1] = split_readData[0:]  # store only measured data and ignore time stamp
    return loadData
