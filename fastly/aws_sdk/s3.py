import boto3
import botocore
import json
import re
import time


class S3:

    def __init__(self):
        self.s3 = boto3.resource('s3')
        self.s3Client = boto3.client('s3')
        self.keylist = []

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

        if ("Contents" in response.keys()):
            for keyObj in response["Contents"]:
                if '.log' in keyObj["Key"]:
                    timestamp = re.search(
                        r"logs\/(\d{4}\/\d{2}\/\d{2}\/\d{2}:\d{2})", keyObj["Key"]).group(1)
                    log_time = time.strptime(
                        timestamp + " UTC", "%Y/%m/%d/%H:%M %Z")
                    if time.mktime(log_time) < time.mktime(gmt):
                        self.keylist.append(keyObj["Key"])

        if response["IsTruncated"] == True:
            self.getlist(
                bucket=bucket,
                prefix=prefix,
                marker=response["NextMarker"],
                gmt=gmt
            )

    def getKeyList(self):
        return self.keylist

    def getObject(self, bucket='', key=''):
        if (bucket == ''):
            raise KeyError('Missing bucket name!')
        elif (key == ''):
            raise KeyError('Missing key value!')
        else:
            try:
                obj = self.s3.Object(bucket, key)
                response = obj.get()
                return response["Body"]
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

    def moveObjects(self, keyList='', bucket='', destBucket='', destFolder=''):
        if (bucket == '' or destBucket == ''):
            raise KeyError('Missing bucket name!')
        elif (destFolder == ''):
            raise KeyError('Missing folder name!')
        elif (keyList == ''):
            raise KeyError('Missing list of S3 Keys!')
        else:
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
                copy_source = {
                    'Bucket': bucket,
                    'Key': key
                }
                destKey = destFolder + '/' + key.split('/')[-1]
                self.s3.meta.client.copy(copy_source, destBucket, destKey)
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

            # for each log file in s3, download it
            for key in keyList:
                body = self.getObject(
                    bucket=bucket,
                    key=key
                )

                # for each log in the file, append it to a jsonObj (dict)
                for lines in body.iter_lines():
                    for line in lines.decode().splitlines():
                        line = line.replace("\\", "\\\\")
                        line = line.replace('\\\\"', '\\"')
                        try:
                            obj = json.loads(line)
                            for objKey in obj.keys():
                                if objKey == 'geo':
                                    for loc in obj[objKey].keys():
                                        if loc in jsonObj:
                                            jsonObj[loc].append(
                                                obj[objKey][loc])
                                        else:
                                            jsonObj[loc] = [obj[objKey][loc]]
                                else:
                                    # **
                                    # if objKey == 'url':
                                    # if re.search(r"\/(\d+)\/", obj[objKey]) and re.search(r"\/(\d+)\/", obj[objKey]).group(1) == '2':
                                    #     channel2.append(line)
                                    # if re.search(r"\/(\d+)\/", obj[objKey]) == None or (re.search(r"\/(\d+)\/", obj[objKey]) and re.search(r"\/(\d+)\/", obj[objKey]).group(1) == ''):
                                    #     emptyChannel.append(line)
                                    # if re.search(r"\/(\d+)\/", obj[objKey]) and re.search(r"\/(\d+)\/", obj[objKey]).group(1) == '59':
                                    #     channel59.append(line)
                                    if objKey in jsonObj:
                                        jsonObj[objKey].append(obj[objKey])
                                    else:
                                        jsonObj[objKey] = [obj[objKey]]
                        except:
                            print(line)

            # **
            # if len(channel2) > 0:
            #     self.putStrObject('prd-freq-report-data-fr', 'fastly_log/2/' +
            #                       time.strftime("%Y%m%d%H%M%S", gmt) + '.txt', '\n'.join(channel2))
            # if len(emptyChannel) > 0:
            #     self.putStrObject('prd-freq-report-data-fr', 'fastly_log/emptyChannel/' +
            #                       time.strftime("%Y%m%d%H%M%S", gmt) + '.txt', '\n'.join(emptyChannel))
            # if len(channel59) > 0:
            #     self.putStrObject('prd-freq-report-data-fr', 'fastly_log/59/' +
            #                       time.strftime("%Y%m%d%H%M%S", gmt) + '.txt', '\n'.join(channel59))
            return jsonObj