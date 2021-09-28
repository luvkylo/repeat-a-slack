import requests
import json
import re
import time
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter


class APIrequests:
    def __init__(self):
        self.s = requests.Session()

        retries = Retry(total=5,
                        backoff_factor=30,
                        status_forcelist=[500, 502, 503, 504])

        self.s.mount('http://', HTTPAdapter(max_retries=retries))

    def regexCheck(self, pattern='', string=''):
        return re.fullmatch(pattern, string)

    def clientErrorCode(self, code, message):
        raise Exception(
            'Error code encountered when making request....' + str(code) + ': ' + message)

    def getOnAirChannel(self, freqID='', freqAuth=''):
        if (freqID == ''):
            raise KeyError('Missing frequency device ID!')
        elif (freqAuth == ''):
            raise KeyError('Missing frequency device auth!')
        else:
            headers = {'X-Frequency-Auth': freqAuth,
                       'X-Frequency-DeviceId': freqID}
            url = 'https://prd-freq.frequency.com/api/2.0/cms/linear_channel?status=ON_AIR&accountId&sort=createdDate.asc'

            response = self.s.get(url=url, headers=headers)

            results = []

            if (response.status_code >= 400 and response.status_code < 500):
                self.clientErrorCode(
                    code=response.status_code, message=response.json()["message"])

            while (response.status_code >= 500 and response.status_code < 600):
                time.sleep(30)
                response = self.s.get(url=url, headers=headers)
            if (response.status_code >= 400 and response.status_code < 500):
                self.clientErrorCode(
                    code=response.status_code, message=response.json()["message"])
            else:
                for channel in response.json():
                    results.append(
                        (channel["linear_channel_id"], channel["account_id"]))

            return results

    def getLinearProgram(self, channel_id='', freqID='', freqAuth='', fromTime='', toTime=''):
        if (channel_id == ''):
            raise KeyError('Missing channel ID!')
        elif (freqID == ''):
            raise KeyError('Missing frequency device ID!')
        elif (freqAuth == ''):
            raise KeyError('Missing frequency device auth!')
        elif (self.regexCheck(r'\w{4}-\w{2}-\w{2}T\w{2}:\w{2}:\w{2}Z', fromTime) is None):
            raise KeyError(
                'fromTime does not match timestamp pattern, consider passing wrong param')
        elif (self.regexCheck(r'\w{4}-\w{2}-\w{2}T\w{2}:\w{2}:\w{2}Z', toTime) is None):
            raise KeyError(
                'toTime does not match timestamp pattern, consider passing wrong param')
        elif (not channel_id.isnumeric()):
            print('Channel ID:', channel_id)
            raise KeyError(
                'Channel ID has incorrect format. Please input correct Channel ID')
        else:
            headers = {'X-Frequency-Auth': freqAuth,
                       'X-Frequency-DeviceId': freqID}
            url = 'https://prd-freq.frequency.com/api/2.0/cms/linear_channel/{channel_id}/schedule?from={fromTime}&to={toTime}'.format(
                channel_id=str(channel_id), fromTime=fromTime, toTime=toTime)

            response = self.s.get(url=url, headers=headers)

            results = []

            if (response.status_code >= 400 and response.status_code < 500):
                self.clientErrorCode(
                    code=response.status_code, message=response.json()["message"])
            else:
                while (response.status_code >= 500 and response.status_code < 600):
                    time.sleep(30)
                    response = self.s.get(url=url, headers=headers)
                if (response.status_code >= 400 and response.status_code < 500):
                    self.clientErrorCode(
                        code=response.status_code, message=response.json()["message"])
                else:
                    for program in response.json()["schedules"]:
                        tempList = [program['type_of_program'],
                                    int(program['linear_schedule_id']),
                                    channel_id,
                                    int(program['program_id']),
                                    program['start_time'],
                                    program['end_time'],
                                    int(program['duration']),
                                    program['title'],
                                    program['description'],
                                    program['series'],
                                    program['season'],
                                    program['episode']]
                        # results.append(['' if v is None else v for v in tempList])
                        results.append(tempList)

            return results

    def getVODProgram(self, account_id='', program_id='', freqID='', freqAuth=''):
        if (account_id == ''):
            raise KeyError('Missing account ID!')
        elif (program_id == ''):
            raise KeyError('Missing program ID!')
        elif (freqID == ''):
            raise KeyError('Missing frequency device ID!')
        elif (freqAuth == ''):
            raise KeyError('Missing frequency device auth!')
        elif (not account_id.isnumeric()):
            print('Account ID:', account_id)
            raise KeyError(
                'Account ID has incorrect format. Please input correct Account ID')
        elif (not isinstance(program_id, int)):
            print('Program ID:', program_id)
            raise KeyError(
                'Program ID has incorrect format. Please input correct Program ID')
        else:
            headers = {'X-Frequency-Auth': freqAuth,
                       'X-Frequency-DeviceId': freqID}
            url = 'https://prd-freq.frequency.com/api/1.0/cms/n/linear/{account_id}/programs/{program_id}'.format(
                account_id=str(account_id), program_id=str(program_id))

            response = self.s.get(url=url, headers=headers)

            if (response.status_code >= 400 and response.status_code < 500):
                self.clientErrorCode(
                    code=response.status_code, message=response.json()["message"])
            while (response.status_code >= 500 and response.status_code < 600):
                time.sleep(30)
                response = self.s.get(url=url, headers=headers)
            if (response.status_code >= 400 and response.status_code < 500):
                self.clientErrorCode(
                    code=response.status_code, message=response.json()["message"])
            else:
                results = []

                if response.json()["linear_program_type"] == 'SINGLE_VIDEO_PROGRAM':
                    results.append(response.json()["video_id"])
                else:
                    for videos in response.json()["components"]:
                        if videos["linear_program_component_type"] == 'VIDEO':
                            if "broken" in videos and videos["broken"] == True:
                                continue
                            else:
                                results.append(videos["video_id"])

                results = [''] if len(results) == 0 else results

            return list(set(results))

    def getAutomationProgram(self, account_id='', auto_program_id='', schedule_id='', freqID='', freqAuth=''):
        if (account_id == ''):
            raise KeyError('Missing account ID!')
        elif (auto_program_id == ''):
            raise KeyError('Missing automation program ID!')
        elif (schedule_id == ''):
            raise KeyError('Missing linear schedule ID!')
        elif (freqID == ''):
            raise KeyError('Missing frequency device ID!')
        elif (freqAuth == ''):
            raise KeyError('Missing frequency device auth!')
        elif (not account_id.isnumeric()):
            print('Account ID:', account_id)
            raise KeyError(
                'Account ID has incorrect format. Please input correct Account ID')
        elif (not isinstance(auto_program_id, int)):
            print('Program ID:', auto_program_id)
            raise KeyError(
                'Program ID has incorrect format. Please input correct Automation Program ID')
        elif (not isinstance(schedule_id, int)):
            print('Schedule ID:', schedule_id)
            raise KeyError(
                'Schedule ID has incorrect format. Please input correct Schedule ID')
        else:
            headers = {'X-Frequency-Auth': freqAuth,
                       'X-Frequency-DeviceId': freqID}
            url = 'https://prd-freq.frequency.com/api/2.0/cms/linear_automation/{auto_program_id}/linear_schedule/{linear_schedule_id}'.format(
                auto_program_id=str(auto_program_id), linear_schedule_id=str(schedule_id))

            response = self.s.get(url=url, headers=headers)

            if (response.status_code >= 400 and response.status_code < 500):
                self.clientErrorCode(
                    code=response.status_code, message=response.json()["message"])
            while (response.status_code >= 500 and response.status_code < 600):
                time.sleep(30)
                response = self.s.get(url=url, headers=headers)
            if (response.status_code >= 400 and response.status_code < 500):
                self.clientErrorCode(
                    code=response.status_code, message=response.json()["message"])
            else:
                results = ['']

                if response.json()['status'] != 'DRAFT':
                    if response.json()['linear_program_id'] == None:
                        linear_program_id = auto_program_id
                    else:
                        linear_program_id = response.json()[
                            'linear_program_id']
                    results = self.getVODProgram(
                        account_id=account_id, program_id=linear_program_id, freqID=freqID, freqAuth=freqAuth)

            return results

    def getDynamicProgram(self, account_id='', dynamic_program_id='', schedule_id='', freqID='', freqAuth=''):
        if (account_id == ''):
            raise KeyError('Missing account ID!')
        elif (dynamic_program_id == ''):
            raise KeyError('Missing dynamic program ID!')
        elif (schedule_id == ''):
            raise KeyError('Missing linear schedule ID!')
        elif (freqID == ''):
            raise KeyError('Missing frequency device ID!')
        elif (freqAuth == ''):
            raise KeyError('Missing frequency device auth!')
        elif (not account_id.isnumeric()):
            print('Account ID:', account_id)
            raise KeyError(
                'Account ID has incorrect format. Please input correct Account ID')
        elif (not isinstance(dynamic_program_id, int)):
            print('Program ID:', dynamic_program_id)
            raise KeyError(
                'Program ID has incorrect format. Please input correct Automation Program ID')
        elif (not isinstance(schedule_id, int)):
            print('Schedule ID:', schedule_id)
            raise KeyError(
                'Schedule ID has incorrect format. Please input correct Schedule ID')
        else:
            headers = {'X-Frequency-Auth': freqAuth,
                       'X-Frequency-DeviceId': freqID,
                       'X-Frequency-Account': account_id}
            url = 'https://prd-freq.frequency.com/api/2.0/cms/dynamic_programs/{dynamic_program_id}/linear_schedules/{linear_schedule_id}'.format(
                dynamic_program_id=str(dynamic_program_id), linear_schedule_id=str(schedule_id))

            response = self.s.get(url=url, headers=headers)

            if (response.status_code >= 400 and response.status_code < 500):
                self.clientErrorCode(
                    code=response.status_code, message=response.json()["message"])
            while (response.status_code >= 500 and response.status_code < 600):
                time.sleep(30)
                response = self.s.get(url=url, headers=headers)
            if (response.status_code >= 400 and response.status_code < 500):
                self.clientErrorCode(
                    code=response.status_code, message=response.json()["message"])
            else:
                results = ['']

                if response.json()['dynamic_program_status'] != 'Draft':
                    if response.json()['linear_program_id'] == None:
                        linear_program_id = dynamic_program_id
                    else:
                        linear_program_id = int(
                            response.json()['linear_program_id'])
                    results = self.getVODProgram(
                        account_id=account_id, program_id=linear_program_id, freqID=freqID, freqAuth=freqAuth)

            return results

    def getFastlyServiceList(self, fastlyKey='', fastlyUrl=''):
        if (fastlyKey == ''):
            raise KeyError('Missing Fastly Key Auth')
        elif (fastlyUrl == ''):
            raise KeyError('Missing Fastly URL')
        else:
            headers = {
                'fastly-key': fastlyKey
            }

            response = self.s.get(url=fastlyUrl, headers=headers)

            services = []

            for service in response.json():
                if re.search(r'(linear-)|(prd-)', service['name'].lower()) and not re.search(r'(qa-)|(test-)', service['name'].lower()):
                    services.append(service['id'])

            return services

    def getFastlyServicesBandwidth(self, fastlyKey='', fastlyUrl='', fastlyServicesList=''):
        if (fastlyKey == ''):
            raise KeyError('Missing Fastly Key Auth')
        elif (fastlyUrl == ''):
            raise KeyError('Missing Fastly URL')
        elif (fastlyServicesList == ''):
            raise KeyError('Missing Fastly Servies List')
        else:
            headers = {
                'fastly-key': fastlyKey
            }

            response = self.s.get(url=fastlyUrl, headers=headers)

            total = 0

            for serviceId in response.json()['data'].keys():
                if serviceId in fastlyServicesList:
                    total += response.json()[
                        'data'][serviceId][0]['bandwidth']

            return total
