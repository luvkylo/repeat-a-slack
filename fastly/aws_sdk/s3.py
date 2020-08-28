import boto3
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

        response = self.s3Client.list_objects(
            Bucket=bucket,
            Prefix=prefix,
            Marker=marker,
            Delimiter='?',
            MaxKeys=1000
        )

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
            obj = self.s3.Object(bucket, key)
            response = obj.get()
            return response["Body"]

    def deleteObject(self, bucket='', key=''):
        if (bucket == ''):
            raise KeyError('Missing bucket name!')
        elif (key == ''):
            raise KeyError('Missing key value!')
        else:
            obj = self.s3.Object(bucket, key)
            obj.delete()

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

    def getDataframeObject(self, keyList='', bucket=''):
        if (bucket == ''):
            raise KeyError('Missing bucket name!')
        elif (keyList == ''):
            raise KeyError('Missing list of S3 Keys!')
        else:
            jsonObj = {}

            # for each log file in s3, download it
            for key in keyList:
                body = self.getObject(
                    bucket=bucket,
                    key=key
                )

                # for each log in the file, append it to a jsonObj (dict)
                for lines in body.iter_lines():
                    for line in lines.decode().splitlines():
                        obj = json.loads(line)
                        for key in obj.keys():
                            if key == 'geo':
                                for loc in obj[key].keys():
                                    if loc in jsonObj:
                                        jsonObj[loc].append(obj[key][loc])
                                    else:
                                        jsonObj[loc] = [obj[key][loc]]
                            else:
                                if key in jsonObj:
                                    jsonObj[key].append(obj[key])
                                else:
                                    jsonObj[key] = [obj[key]]

            return jsonObj
