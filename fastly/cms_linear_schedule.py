import json
import time
import calendar
import sys
import hashlib
import re
from datetime import datetime

from service import env
from service import api

from db import sql
from db import query


def main():
    start = time.gmtime(time.time())
    # this is the query end time (i.e. this is 2020-10-28T01:00:00Z)
    # start = time.strptime("2021-01-25 00:00:00 +0000", "%Y-%m-%d %H:%M:%S %z")
    startStr = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    print('Script start at', startStr)

    newPyCompleted = time.strptime(time.strftime(
        "%Y-%m-%d %H:00:00", start), "%Y-%m-%d %H:%M:%S")
    newCompleted = time.strftime("%Y-%m-%dT%H:00:00Z", start)

    env_var = env.Env()
    queries = query.Queries()
    apis = api.APIrequests()

    print("Connecting to Redshift...")
    # connect to Redshift
    # On QA, this will be connected to the QA reporting Redshift database
    # On PROD, this will be connected to the PROD reporting Redshift database
    redshift = sql.Redshift(
        user=env_var.redshift_user,
        password=env_var.redshift_pw,
        host=env_var.redshift_host,
        database=env_var.redshift_db,
        port=env_var.redshift_port
    )

    print("Getting last timestamp...")

    redshift.execute(queries.getLastCompletedTime(
        job_name='cms_linear_schedule'))

    # This is the query start time (i.e. this is 2020-10-28T00:00:00Z)
    # completed = time.strftime(
    #     "%Y-%m-%dT%H:%M:%SZ", time.strptime("2021-03-03 05:00:00 +0000", "%Y-%m-%d %H:%M:%S %z"))
    completed = redshift.returnResult()[0][0].strftime("%Y-%m-%dT%H:%M:%SZ")

    print("Script last completed at", completed)

    pyCompleted = time.strptime(completed, "%Y-%m-%dT%H:%M:%SZ")

    try:
        if pyCompleted < newPyCompleted:

            hashed_id = hashlib.sha256(
                (startStr + 'cms_linear_schedule').encode('utf-8')).hexdigest()

            print("Creating log entry...")

            redshift.execute(queries.initiateLog(
                hashed_id=hashed_id, startStr=startStr, job_name='cms_linear_schedule'))

            print("Created log entry")
            print("************************************************************")

            print("Querying to get all Channel ID")
            print("Query start: " + completed)
            print("Query end: " + newCompleted)

            IDs = apis.getOnAirChannel(
                freqID=env_var.x_frequency_deviceid, freqAuth=env_var.x_frequency_auth)

            insertValues = []

            for (channel_id, account_id) in IDs:
                print("Getting Linear Program from channel", channel_id)
                programList = apis.getLinearProgram(channel_id=channel_id,
                                                    freqID=env_var.x_frequency_deviceid, freqAuth=env_var.x_frequency_auth, fromTime=completed, toTime=newCompleted)

                if len(programList) == 0:
                    print("Cannot find any program")
                else:
                    print("Getting Videos...")

                    for li in programList:
                        newList = li[1:]
                        programType = li[0]
                        schedule_id = li[1]
                        program_id = li[3]

                        print("Checking if program is in database:", program_id)
                        redshift.execute(
                            queries.checkScheduleID(schedule_id=schedule_id)
                        )
                        count = redshift.returnResult()

                        if count[0][0] > 0:
                            print(
                                "This program is already in the database:", program_id)
                        else:
                            print("Program is not in the database yet")
                            if programType == 'LIVE STREAM':
                                insertValues.append(
                                    [account_id] + newList + [None, startStr])
                            elif programType == 'VOD_PROGRAM':
                                videoIDs = apis.getVODProgram(
                                    account_id=account_id, program_id=program_id, freqID=env_var.x_frequency_deviceid, freqAuth=env_var.x_frequency_auth)
                                for video_id in videoIDs:
                                    video_id = None if video_id == '' else int(
                                        video_id)
                                    insertValues.append(
                                        [account_id] + newList + [video_id, startStr])
                            elif programType == 'AUTOMATION_PROGRAM':
                                videoIDs = apis.getAutomationProgram(account_id=account_id, auto_program_id=program_id,
                                                                     schedule_id=schedule_id, freqID=env_var.x_frequency_deviceid, freqAuth=env_var.x_frequency_auth)
                                for video_id in videoIDs:
                                    video_id = None if video_id == '' else int(
                                        video_id)
                                    insertValues.append(
                                        [account_id] + newList + [video_id, startStr])
                            elif programType == 'DYNAMIC_PROGRAM':
                                videoIDs = apis.getDynamicProgram(account_id=account_id, auto_program_id=program_id,
                                                                  schedule_id=schedule_id, freqID=env_var.x_frequency_deviceid, freqAuth=env_var.x_frequency_auth)
                                for video_id in videoIDs:
                                    video_id = None if video_id == '' else int(
                                        video_id)
                                    insertValues.append(
                                        [account_id] + newList + [video_id, startStr])
                            else:
                                print("Do not recognize program type:",
                                      programType)
                                insertValues.append(
                                    [account_id] + newList + [None, startStr])

            print("************************************************************")

            args_str = b','.join(redshift.cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", x)
                                 for x in tuple(insertValues))

            args_str = args_str.decode(
                "utf-8").replace('::timestamp', '').replace('"', '\\"').replace('“', '\\"').replace('”', '\\"')
            args_str = re.sub('\s+', ' ', args_str)

            print("Writting the results into the database...")

            while len(args_str) > 15000000:
                index = args_str.find(")", 15000000, 16000000)
                temp_str = args_str[0: index + 1]
                args_str = args_str[index + 2:]
                redshift.execute("INSERT INTO cms_linear_schedule (account_id, linear_schedule_id, linear_channel_id, linear_program_id, schedule_start_time, schedule_end_time, schedule_duration_ms, linear_program_title, linear_program_description, series, season, episode, video_id, updated_at) VALUES " + temp_str)
            if len(args_str) > 0:
                redshift.execute("INSERT INTO cms_linear_schedule (account_id, linear_schedule_id, linear_channel_id, linear_program_id, schedule_start_time, schedule_end_time, schedule_duration_ms, linear_program_title, linear_program_description, series, season, episode, video_id, updated_at) VALUES " + args_str)

            print("Data ingested")

            print("************************************************************")

            endStr = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            completedStr = time.strftime("%Y-%m-%dT%H:00:00Z", start)

            redshift.execute(
                queries.completedLog(hashed_id=hashed_id,
                                     endStr=endStr, completedStr=completedStr, job_name='cms_linear_schedule')
            )

            print("Log updated...closing connection")

            redshift.closeEverything()
            print("Connection closed")
        else:
            print("No new query")
            redshift.closeEverything()
            # print("Connection closed")
            # raise KeyError("No new query")
    except:
        redshift.execute(queries.errorLog(
            hashed_id=hashed_id, error=str(sys.exc_info()[1]), job_name='cms_linear_schedule'))

        redshift.closeEverything()

        raise sys.exc_info()[0]


if __name__ == "__main__":
    main()
