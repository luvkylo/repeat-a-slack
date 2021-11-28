import boto3
import botocore
import json
import re
import time
import os
import gzip
from os.path import dirname, abspath
from datetime import datetime
from concurrent import futures

import pandas as pd


class S3:

    def __init__(self):
        self.s3 = boto3.resource('s3')
        self.s3Client = boto3.client('s3')
        self.keylist = []

    def match(self, regex, x, group=1):
        if re.search(regex, x):
            return re.search(regex, x).group(group)
        return ''

    def getlist(self, bucket='', prefix='', marker='', gmt=''):
        if (bucket == ''):
            raise KeyError('Missing bucket name!')
        if (gmt == ''):
            raise KeyError('Missing UTC timestamp!')
        if (prefix != '' and prefix[-1] != '/'):
            prefix = prefix + '/'

        try:
            response = self.s3Client.list_objects(
                Bucket=bucket,
                Prefix=prefix,
                Marker=marker,
                Delimiter='?',
                MaxKeys=1000
            )
        except botocore.exceptions.ClientError as error:
            raise error

        processingPrefix = time.strftime("%Y%m%d_%H:%M:%S", gmt)

        try:
            if ("Contents" in response.keys()):
                for keyObj in response["Contents"]:
                    if '.log' in keyObj["Key"]:
                        timestamp = re.search(
                            r"logs\/(\d{4}\/\d{2}\/\d{2}\/\d{2}:\d{2})", keyObj["Key"]).group(1)
                        log_time = time.strptime(
                            timestamp + " UTC", "%Y/%m/%d/%H:%M %Z")
                        # log_las_modified_time = keyObj["LastModified"].timetuple()
                        if any([time.mktime(log_time) < time.mktime(gmt)]):
                            self.s3.Object(bucket, keyObj["Key"].replace(
                                'logs/', 'processing/' + processingPrefix + '/')).copy_from(CopySource=bucket + '/' + keyObj["Key"])
                            self.s3.Object(bucket, keyObj["Key"]).delete()
                            self.keylist.append(keyObj["Key"].replace(
                                'logs/', 'processing/' + processingPrefix + '/'))

            if response["IsTruncated"] == True:
                self.getlist(
                    bucket=bucket,
                    prefix=prefix,
                    marker=response["NextMarker"],
                    gmt=gmt
                )
        except BaseException as e:
            processingPrefix = time.strftime("%Y%m%d_%H:%M:%S", gmt)

            for key in self.keyList:
                self.s3.Object(bucket, key.replace(
                    'processing/' + processingPrefix + '/', 'logs/')).copy_from(CopySource=bucket + '/' + key)
                self.s3.Object(bucket, key).delete()

    def getKeyList(self):
        return self.keylist

    def getObject(self, args=''):
        if (args == '' or len(args) < 2):
            raise KeyError('Missing arguments!')
        else:
            bucket = args[0]
            key = args[1]
            try:
                print(f'downloading key {key}')
                obj = self.s3.Object(bucket, key)
                response = obj.get()
                body = response["Body"].iter_lines()
                jsonObj = self.createJsonObj(body)
                print(f'Finished key {key}')
                return jsonObj
            except self.s3.meta.client.exceptions.NoSuchKey as err:
                print("Failed to download object")
                print("Key:", key, "\ndoes not exist in the bucket:", bucket)
                ValueError(err)
            except self.s3.meta.client.exceptions.NoSuchBucket as err:
                print("Failed to download object")
                print("Bucket does not exist:", bucket)
                ValueError(err)

    def putStrObject(self, bucket='', key='', string=''):
        if (bucket == ''):
            raise KeyError('Missing bucket name!')
        elif (key == ''):
            raise KeyError('Missing key value!')
        elif (string == ''):
            raise KeyError('Missing string body value!')
        else:
            try:
                obj = self.s3.Object(bucket, key)
                obj.put(Body=string)
            except self.s3.meta.client.exceptions.NoSuchKey as err:
                print("Failed to delete object")
                print("Key:", key, "\ndoes not exist in the bucket:", bucket)
                raise ValueError(err)
            except self.s3.meta.client.exceptions.NoSuchBucket as err:
                print("Failed to delete object")
                print("Bucket does not exist:", bucket)
                raise ValueError(err)

    def deleteObject(self, bucket='', key=''):
        if (bucket == ''):
            raise KeyError('Missing bucket name!')
        elif (key == ''):
            raise KeyError('Missing key value!')
        else:
            try:
                obj = self.s3.Object(bucket, key)
                obj.delete()
            except self.s3.meta.client.exceptions.NoSuchKey as err:
                print("Failed to delete object")
                print("Key:", key, "\ndoes not exist in the bucket:", bucket)
                raise ValueError(err)
            except self.s3.meta.client.exceptions.NoSuchBucket as err:
                print("Failed to delete object")
                print("Bucket does not exist:", bucket)
                raise ValueError(err)

    def deleteObjects(self, keyList='', bucket=''):
        if (bucket == ''):
            raise KeyError('Missing bucket name!')
        elif (keyList == ''):
            raise KeyError('Missing list of S3 Keys!')
        else:
            for key in keyList:
                self.deleteObject(
                    bucket=bucket,
                    key=key
                )
            print("All Files are removed")

    def uploadObject(self, args=''):
        if (args == '' or len(args) < 6):
            raise KeyError('Missing arguments!')
        else:
            id = args[0]
            keyList = args[1]
            tempDf = args[2]
            destBucket = args[3]
            destFolder = args[4]
            start = args[5]

            directory = dirname(abspath(__file__))
            year = keyList[0].split('/')[-4]
            month = keyList[0].split('/')[-3]
            day = keyList[0].split('/')[-2]
            timestamp = keyList[0].split('/')[-1].split(' ')[0].replace('.00', '').replace(':', '')
            current = time.strftime("%Y%m%d_%H%M%S", start)
            filename = f'{year}{month}{day}_{timestamp}_{current}.csv'

            os.mkdir(os.path.join(directory, id))
            tempDf.to_csv(os.path.join(directory, id, filename), index=False)
            destKey = f'{destFolder}/{id}/{year}/{month}/{day}/{timestamp}_{current}.csv'

            print("Uploading files for channel: " + id + "...")
            self.s3.meta.client.upload_file(os.path.join(directory, id, filename), destBucket, destKey)
            print(f"File for channel {id} uploaded")
            print(f"Removing local files for channel {id} now")
            if os.path.exists(os.path.join(directory, id, filename)):
                os.remove(os.path.join(directory, id, filename))
                os.rmdir(os.path.join(directory, id))
            else:
                print(f"Local file for channel {id} does not exist")
            return id

    def moveObjects(self, keyList='', bucket='', destBucket='', destFolder='', jsonObj=''):
        if (bucket == '' or destBucket == ''):
            raise KeyError('Missing bucket name!')
        elif (destFolder == ''):
            raise KeyError('Missing folder name!')
        elif (keyList == ''):
            raise KeyError('Missing list of S3 Keys!')
        else:
            df = pd.DataFrame.from_dict(jsonObj)
            df = df.drop(columns=['client_ip'])
            df['channel_id'] = df['url'].apply(
                lambda x: self.match(r"\/(\d+)\/", x))

            start = time.gmtime(time.time())

            uniqueChannelId = df['channel_id'].unique()

            uploadObjectArgs = []

            for id in uniqueChannelId:

                tempDf = df[(df['channel_id'] == id)]
                tempDf = tempDf.drop(columns=['channel_id'])

                if (id == ''):
                    id = 'unknown'

                uploadObjectArgs.append(
                    (id, keyList, tempDf, destBucket, destFolder, start))

            with futures.ThreadPoolExecutor() as executor:
                res = executor.map(self.uploadObject, uploadObjectArgs)

                for id in res:
                    print(f"Local file for channel {id} removed")

            print("Removing S3 files now")
            for key in keyList:
                self.moveObject(
                    bucket=bucket,
                    destBucket=destBucket,
                    destFolder=destFolder,
                    key=key
                )
            print("All Files are removed")

    def moveObject(self, bucket='', destBucket='', destFolder='', key=''):
        if (bucket == '' or destBucket == ''):
            raise KeyError('Missing bucket name!')
        elif (destFolder == ''):
            raise KeyError('Missing folder name!')
        elif (key == ''):
            raise KeyError('Missing key value!')
        else:
            try:
                self.deleteObject(
                    bucket=bucket,
                    key=key
                )
            except self.s3.meta.client.exceptions.NoSuchKey as err:
                print("Failed to delete object")
                print("Key:", key, "\ndoes not exist in the bucket:", bucket)
                raise ValueError(err)
            except self.s3.meta.client.exceptions.NoSuchBucket as err:
                print("Failed to delete object")
                print("Bucket does not exist:", bucket)
                raise ValueError(err)

    def flat_keys(self, obj):
        keys = []
        for k, v in obj.items():
            if type(v) is dict:
                keys = keys + self.flat_keys(v)
            else:
                keys.append(k)
        return keys

    def createJsonObj(self, body):
        jsonObj = {}
        emptyLog = 0

        for lines in body:
            for line in lines.decode(encoding="utf-8", errors="backslashreplace").splitlines():
                line = line.replace("\\", "\\\\")
                line = line.replace('\\\\"', '\\"')
                try:
                    obj = json.loads(line)
                    emptyLog += 1
                    for objKey in obj.keys():
                        if objKey == 'geo':
                            for loc in obj[objKey].keys():
                                if loc in jsonObj:
                                    jsonObj[loc].append(
                                        obj[objKey][loc])
                                else:
                                    jsonObj[loc] = [obj[objKey][loc]]
                        else:
                            if objKey in jsonObj:
                                jsonObj[objKey].append(obj[objKey])
                            else:
                                jsonObj[objKey] = [''] * (emptyLog - 1)
                                jsonObj[objKey].append(obj[objKey])
                    for key in jsonObj.keys():
                        if key not in self.flat_keys(obj):
                            jsonObj[key].append('')
                except:
                    print(line)
        
        if "client_request" not in jsonObj:
            jsonObj["client_request"] = [''] * emptyLog

        return jsonObj

    def getDataframeObject(self, keyList='', bucket='', gmt=''):
        if (bucket == ''):
            raise KeyError('Missing bucket name!')
        elif (keyList == ''):
            raise KeyError('Missing list of S3 Keys!')
        else:
            jsonObj = {}

            # capture channel 2 and empty channel id for research purpose (**)
            channel2 = []
            channel59 = []
            emptyChannel = []

            executorArgs = [(bucket, key) for key in keyList]

            with futures.ThreadPoolExecutor() as executor:
                executeRes = executor.map(self.getObject, executorArgs)

            print('completed all file download')
            # for each log file in s3, download it

            emptyLog = 0

            for body in executeRes:
                # for each log in the file, append it to a jsonObj (dict)
                for objKey in body.keys():
                    if objKey in jsonObj:
                        jsonObj[objKey] = jsonObj[objKey] + body[objKey]
                    else:
                        jsonObj[objKey] = [''] * (emptyLog - 1)
                        jsonObj[objKey] = jsonObj[objKey] + body[objKey]
                emptyLog += len(body[list(body.keys())[0]])
            return jsonObj

    def getOriginBandwidthFilelist(self, bucket='', prefix='', marker='', gmt=''):
        if (bucket == ''):
            raise KeyError('Missing bucket name!')
        if (gmt == ''):
            raise KeyError('Missing UTC timestamp!')
        if (prefix != '' and prefix[-1] != '/'):
            prefix = prefix + '/'

        try:
            response = self.s3Client.list_objects(
                Bucket=bucket,
                Prefix=prefix,
                Marker=marker,
                Delimiter='?',
                MaxKeys=1000
            )
        except botocore.exceptions.ClientError as error:
            raise error

        # d = datetime.fromtimestamp(time.mktime(gmt))
        # month = str(d.month) if d.month >= 10 else '0' + str(d.month)
        # fileTime = str(d.year) + '-' + month

        if ("Contents" in response.keys()):
            for keyObj in response["Contents"]:
                if 'Frequency-Tag-BillingId' in keyObj["Key"]:
                    timestamp = re.search(
                        r"(\d{4}-\d{2})", str(keyObj["LastModified"])).group(1)
                    log_time = time.strptime(
                        timestamp + "-01 00:00 UTC", "%Y-%m-%d %H:%M %Z")
                    if any([time.mktime(log_time) >= time.mktime(gmt)]):
                        self.keylist.append(keyObj["Key"])

        if response["IsTruncated"] == True:
            self.getlist(
                bucket=bucket,
                prefix=prefix,
                marker=response["NextMarker"],
                gmt=gmt
            )

    def getOriginBandwidthObject(self, keyList='', bucket='', t='', productList=''):
        if (bucket == ''):
            raise KeyError('Missing bucket name!')
        elif (keyList == ''):
            raise KeyError('Missing list of S3 Keys!')
        else:
            result = []

            executorArgs = [(bucket, key) for key in keyList]

            with futures.ThreadPoolExecutor() as executor:
                executeRes = executor.map(self.getObject, executorArgs)

            # for each log file in s3, download it
            for body in executeRes:

                with gzip.GzipFile(fileobj=body) as gzipfile:
                    content = gzipfile.read()

                i = 0

                for line in content.decode(encoding="utf-8", errors="backslashreplace").splitlines():
                    i += 1
                    if i > 1:

                        rawLine = line.split(",")
                        itemDescription = rawLine[3]
                        productName = rawLine[1]
                        dateList = rawLine[-3:]
                        dateList.reverse()

                        log_time = time.strptime(
                            '-'.join(dateList) + " 00:00 UTC", "%Y-%m-%d %H:%M %Z")

                        conditions = [
                            productName in productList and 'data transfer out' in itemDescription.lower(),
                            productName == 'AWS Elemental MediaConnect' and rawLine[
                                2] == 'Data Transfer: Transfer' and 'outbound' in itemDescription.lower()
                        ]

                        if any(conditions):
                            timestamps = time.strftime(
                                "%Y-%m-%dT%H:%M:%SZ", log_time)
                            billingId = rawLine[0].split(":")
                            if len(billingId) < 3:
                                channelId = 'untagged'
                                accountId = 'untagged'
                                billableParty = 'untagged'
                            else:
                                channelId = billingId[0]
                                accountId = billingId[1]
                                billableParty = billingId[2]

                            if len(billingId) < 4:
                                distributor = ''
                            elif len(billingId) == 4:
                                distributor = billingId[3]
                            usageQuantity = str(round(float(rawLine[4]), 2))
                            cost = str(round(float(rawLine[5]), 6))

                            lineItem = (timestamps, channelId, accountId, billableParty,
                                        distributor, productName, itemDescription, usageQuantity, cost)
                            result.append(lineItem)

            # get [["sda", asd], ["asdas", asdas]]
            return result
