import json
import time
import calendar
import sys
import hashlib
from datetime import datetime

from service import env

from db import sql


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

    print("Connecting to Redshift...")
    # connect to Redshift
    redshift = sql.Redshift(
        user=env_var.redshift_user,
        password=env_var.redshift_pw,
        host=env_var.redshift_host,
        database=env_var.redshift_db,
        port=env_var.redshift_port
    )

    redshift1 = sql.Redshift(
        user=env_var.redshift_user,
        password=env_var.redshift_pw1,
        host=env_var.redshift_host1,
        database=env_var.redshift_db,
        port=env_var.redshift_port
    )

    print("Getting last timestamp...")

    redshift.execute(
        """
            SELECT next_job_start_time
            FROM reporting_cron_job_logs
            WHERE job_name='fastly_log_with_video_and_schedule_metadata' and status='DONE'
            ORDER BY cron_end_time DESC;
        """
    )

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

            redshift.execute(
                """
                    INSERT INTO reporting_cron_job_logs (hashed_id, cron_start_time, job_name, status) VALUES ('{hashed_id}','{startStr}','fastly_log_with_video_and_schedule_metadata','RUNNING')
                """.format(hashed_id=hashed_id, startStr=startStr)
            )

            print("Created log entry")
            print("************************************************************")

            print("Querying all tables...")
            print("Query start: " + completed)
            print("Query end: " + newCompleted)
            print("Query one hour prior: " + oneHourPrior)
            print("Query one hour later: " + oneHourLater)

            redshift1.execute(
                """
                SELECT
                schedule.channel_id as id,
                CASE WHEN linear.channel_name IS NULL THEN 'Unknown' ELSE linear.channel_name END as channel_name,
                schedule.program_start_time as program_start_time,
                schedule.program_end_time as program_end_time,
                schedule.program_title as program_title,
                schedule.video_title as video_title,
                schedule.video_description as video_description,
                schedule.video_start_time as video_start_time,
                schedule.video_end_time as video_end_time,
                ROW_NUMBER() OVER (PARTITION BY external_id, program_start_time, id
                                                ORDER BY video_start_time desc) AS video_ranked,
                schedule.external_id as external_id,
                schedule.frequency_id as frequency_id,
                upper(split_part(split_part(logs.distributor,'-',2), '/', 1)) as distributor,
                sum(logs.minutes_watched) as minutes_watched
                FROM (
                SELECT timestamps, channel_id, distributor, minutes_watched
                FROM fastly_log_aggregated_metadata
                WHERE timestamps>='{time1}' and timestamps<'{time2}'
                ) as logs
                LEFT JOIN (
                SELECT
                    program.linear_channel_id as channel_id,
                    program.schedule_start_time as program_start_time,
                    program.schedule_end_time as program_end_time,
                    video.linear_program_title as program_title,
                    v.video_title as video_title,
                    v.video_description as video_description,
                    video.start_time as video_start_time,
                    video.end_time as video_end_time,
                    v.video_source_id as external_id,
                    video.instruction_reference_asset_id as frequency_id
                FROM (
                    SELECT linear_channel_id, schedule_start_time, schedule_end_time, linear_program_id
                    FROM
                    (SELECT distinct linear_channel_id, schedule_start_time, schedule_end_time, linear_program_id,
                            ROW_NUMBER() OVER (PARTITION BY linear_channel_id, schedule_start_time, schedule_end_time
                                                ORDER BY schedule_update_date desc) AS ranked_num
                    FROM cms_linear_schedule_master
                    WHERE schedule_start_time>='{time1}' and schedule_start_time<'{time2}' and schedule_status<>'REMOVED'
                    ORDER BY schedule_start_time) AS ranked
                    WHERE ranked.ranked_num = 1
                ) as program
                LEFT JOIN (
                    SELECT
                    linear_program_title,
                    linear_program_description,
                    instruction_start_time,
                    CASE WHEN DATE_PART(min, instruction_start_time) >= 30 THEN DATE_TRUNC('minutes', DATEADD(min, 1, instruction_start_time)) ELSE DATE_TRUNC('minutes', instruction_start_time) END as start_time,
                    CASE WHEN DATE_PART(min, DATEADD('ms', instruction_duration_ms, instruction_start_time)) >= 30 THEN DATE_TRUNC('minutes', DATEADD(min, 1, DATEADD('ms', instruction_duration_ms, instruction_start_time))) ELSE DATE_TRUNC('minutes', DATEADD('ms', instruction_duration_ms, instruction_start_time)) END as end_time,
                    instruction_reference_asset_id,
                    linear_program_id
                    FROM (
                    SELECT linear_program_title, linear_channel_id, linear_program_description, instruction_start_time, instruction_reference_asset_id, instruction_duration_ms, linear_program_id, instruction_reference_start_ms,
                        ROW_NUMBER() OVER (PARTITION BY linear_channel_id, linear_program_title, instruction_reference_asset_id, instruction_start_time
                                        ORDER BY event_time desc) AS ranked_num
                    FROM cms_linear_schedule_master
                    WHERE instruction_type='VIDEO' OR instruction_type='INTERSTITIAL' and event_type='CREATE' and instruction_start_time>='{time3}' and instruction_start_time<'{time4}'
                    ORDER BY instruction_start_time) AS ranked
                    WHERE ranked.ranked_num = 1 and start_time>='{time1}' and start_time<'{time2}'
                ) as video
                ON video.linear_program_id=program.linear_program_id and video.start_time>=program.schedule_start_time and video.end_time<=program.schedule_end_time
                LEFT JOIN video_all_data v
                ON video.instruction_reference_asset_id=v.video_id
                ) as schedule
                ON logs.timestamps >= schedule.video_start_time and logs.timestamps < schedule.video_end_time and logs.channel_id=schedule.channel_id
                LEFT JOIN (
                    WITH ld AS (
                    SELECT linear_channel_id, max("last_modified_date") AS latest
                    FROM cms_linear_channel
                    GROUP BY linear_channel_id
                )
                SELECT linear.linear_channel_id, linear.title as channel_name
                FROM cms_linear_channel AS linear
                JOIN ld ON ld.linear_channel_id = linear.linear_channel_id
                WHERE linear."last_modified_date" = ld.latest
                ) as linear
                on linear.linear_channel_id=schedule.channel_id
                GROUP BY id, channel_name, program_start_time, program_end_time, program_title, video_title, video_description, video_start_time, video_end_time, external_id, frequency_id, distributor
                """.format(time1=completed, time2=newCompleted, time3=oneHourPrior, time4=oneHourLater)
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
                """
                    UPDATE reporting_cron_job_logs
                    SET status='DONE', cron_end_time='{endStr}', next_job_start_time='{completedStr}'
                    WHERE job_name='fastly_log_with_video_and_schedule_metadata' AND hashed_id='{hashed_id}'
                """.format(hashed_id=hashed_id, endStr=endStr, completedStr=completedStr)
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
        redshift.execute(
            """
                UPDATE reporting_cron_job_logs
                SET status='ERROR' and error_note='error'
                WHERE job_name='fastly_log_with_video_and_schedule_metadata' AND hashed_id='{hashed_id}'
            """.format(hashed_id=hashed_id, error=sys.exc_info()[0])
        )

        redshift.closeEverything()

        sys.exit(-1)


if __name__ == "__main__":
    main()
