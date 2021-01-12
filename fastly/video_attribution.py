import json
import time
import calendar
import sys
import hashlib
from datetime import datetime

from service import env

from db import sql
from db import queries


def main():
    start = time.gmtime(time.time() - 10800)
    # this is the query end time (i.e. this is 2020-10-28T01:00:00Z)
    # start = time.strptime("2020-11-01 00:00:00 +0000", "%Y-%m-%d %H:%M:%S %z")
    startStr = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    print('Script start at', startStr)

    newPyCompleted = time.strptime(time.strftime(
        "%Y-%m-%d %H:00:00", start), "%Y-%m-%d %H:%M:%S")
    oneHourLater = time.strftime(
        "%Y-%m-%d %H:00:00", time.gmtime((calendar.timegm(newPyCompleted)+7200)))
    newCompleted = time.strftime("%Y-%m-%d %H:00:00", start)

    env_var = env.Env()
    queries = queries.Queries()

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

    # On both QA and PROD, this will be connected to the PROD reporting Redshift database
    # QA redshift does not have the required data in the cms_linear_svhedule_master and
    # video_all_data so I am querying directly from the PROD reporting redshift and
    # inserting into the QA reporting redshift for easier testing on Eric side when
    # comparing data
    redshift1 = sql.Redshift(
        user=env_var.redshift_user,
        password=env_var.redshift_pw1,
        host=env_var.redshift_host1,
        database=env_var.redshift_db,
        port=env_var.redshift_port
    )

    print("Getting last timestamp...")

    redshift.execute(queries.getLastCompletedTime())

    # This is the query start time (i.e. this is 2020-10-28T00:00:00Z)
    # completed = time.strftime(
    #     "%Y-%m-%d %H:%M:%S", time.strptime("2020-10-28 00:00:00 +0000", "%Y-%m-%d %H:%M:%S %z"))
    completed = redshift.returnResult()[0][0].strftime("%Y-%m-%d %H:%M:%S")

    print("Script last completed at", completed)

    pyCompleted = time.strptime(completed + " +0000", "%Y-%m-%d %H:%M:%S %z")

    oneHourPrior = time.strftime(
        "%Y-%m-%d %H:00:00", time.gmtime((calendar.timegm(pyCompleted)-7200)))

    try:
        if pyCompleted < newPyCompleted:

            hashed_id = hashlib.sha256(
                (startStr + 'fastly_log_with_video_and_schedule_metadata').encode('utf-8')).hexdigest()

            print("Creating log entry...")

            redshift.execute(queries.initiateLog(
                hashed_id=hashed_id, startStr=startStr))

            print("Created log entry")
            print("************************************************************")

            print("Querying all tables...")
            print("Query start: " + completed)
            print("Query end: " + newCompleted)
            print("Query one hour prior: " + oneHourPrior)
            print("Query one hour later: " + oneHourLater)

            redshift1.execute(
                query.fastlyLogWithVideoAndScheduleQuery(
                    completed=completed, newCompleted=newCompleted, oneHourPrior=oneHourPrior, oneHourLater=oneHourLater)
            )

            args_str = b','.join(redshift.cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", x)
                                 for x in tuple(redshift1.returnResult()))

            args_str = args_str.decode("utf-8").replace('::timestamp', '')

            while len(args_str) > 15000000:
                index = args_str.find(")", 15000000, 16000000)
                temp_str = args_str[0: index + 1]
                args_str = args_str[index + 2:]
                redshift.execute("INSERT INTO fastly_log_with_video_and_schedule_metadata (id, channel_name, program_start_time, program_end_time, program_title, video_title, video_description, video_start_time, video_end_time, video_ranked, external_id, frequency_id, distributor, minutes_watched) VALUES " + temp_str)
            if len(args_str) > 0:
                redshift.execute("INSERT INTO fastly_log_with_video_and_schedule_metadata (id, channel_name, program_start_time, program_end_time, program_title, video_title, video_description, video_start_time, video_end_time, video_ranked, external_id, frequency_id, distributor, minutes_watched) VALUES " + args_str)

            print("Data ingested")

            print("************************************************************")

            endStr = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            completedStr = time.strftime("%Y-%m-%dT%H:00:00Z", start)

            redshift.execute(
                queries.completedLog(hashed_id=hashed_id,
                                     endStr=endStr, completedStr=completedStr)
            )

            print("Log updated...closing connection")

            redshift.closeEverything()
            print("Connection closed")
        else:
            print("No new query")
            redshift.closeEverything()
            print("Connection closed")
            sys.exit(-1)
    except:
        redshift.execute(queries.errorLog(
            hashed_id=hashed_id, error=sys.exc_info()[0]))

        redshift.closeEverything()

        sys.exit(-1)


if __name__ == "__main__":
    main()
