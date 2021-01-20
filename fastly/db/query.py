import re


class Queries:
    def __init__(self):
        pass

    def regexCheck(self, pattern, string):
        return re.fullmatch(pattern, string)

    def groupRegexCheck(self, pattern, lis):
        res = []
        for var in lis:
            res.append(re.fullmatch(pattern, var))
        return res

    def getLastCompletedTime(self):
        return """
            SELECT next_job_start_time
            FROM reporting_cron_job_logs
            WHERE job_name='fastly_log_with_video_and_schedule_metadata' and status='DONE'
            ORDER BY cron_end_time DESC;
        """

    def initiateLog(self, hashed_id='', startStr=''):
        if (hashed_id == ''):
            raise KeyError('Missing hashed_id!')
        elif (startStr == ''):
            raise KeyError('Missing cron start time!')
        elif (len(hashed_id) != 64):
            raise KeyError(
                'hashed_id is not 64 characters long, consider passing wrong param')
        elif (self.regexCheck(r'\w{4}-\w{2}-\w{2}T\w{2}:\w{2}:\w{2}Z', startStr) is None):
            raise KeyError(
                'startStr does not match timestamp pattern, consider passing wrong param')
        else:
            return """
                    INSERT INTO reporting_cron_job_logs (hashed_id, cron_start_time, job_name, status) VALUES ('{hashed_id}','{startStr}','fastly_log_with_video_and_schedule_metadata','RUNNING')
                """.format(hashed_id=hashed_id, startStr=startStr)

    def completedLog(self, hashed_id='', endStr='', completedStr=''):
        if (hashed_id == ''):
            raise KeyError('Missing hashed_id!')
        elif (endStr == ''):
            raise KeyError('Missing cron end time!')
        elif (completedStr == ''):
            raise KeyError('Missing next job start time!')
        elif (len(hashed_id) != 64):
            raise KeyError(
                'hashed_id is not 64 characters long, consider passing wrong param')
        elif (None in self.groupRegexCheck(r'\w{4}-\w{2}-\w{2}T\w{2}:\w{2}:\w{2}Z', [endStr, completedStr])):
            raise KeyError(
                'endStr or completedStr does not match timestamp pattern, consider passing wrong param')
        else:
            return """
                    UPDATE reporting_cron_job_logs
                    SET status='DONE', cron_end_time='{endStr}', next_job_start_time='{completedStr}'
                    WHERE job_name='fastly_log_with_video_and_schedule_metadata' AND hashed_id='{hashed_id}'
                """.format(hashed_id=hashed_id, endStr=endStr, completedStr=completedStr)

    def errorLog(self, hashed_id='', error=''):
        if (hashed_id == ''):
            raise KeyError('Missing hashed_id!')
        elif (error == ''):
            raise KeyError('Missing error!')
        elif (len(hashed_id) != 64):
            raise KeyError(
                'hashed_id is not 64 characters long, consider passing wrong param')
        else:
            return """
                UPDATE reporting_cron_job_logs
                SET status='ERROR', error_note='{error}'
                WHERE job_name='fastly_log_with_video_and_schedule_metadata' AND hashed_id='{hashed_id}'
            """.format(hashed_id=hashed_id, error=error.replace("'", "\\'"))

    def fastlyLogWithVideoAndScheduleQuery(self, completed='', newCompleted='', onePrior='', oneLater=''):
        if (completed == '' or newCompleted == '' or onePrior == '' or oneLater == ''):
            raise KeyError('Missing one of the param!!')
        elif (None in self.groupRegexCheck(r'\w{4}-\w{2}-\w{2}\s{1}\w{2}:\w{2}:\w{2}', [completed, newCompleted, onePrior, oneLater])):
            raise KeyError(
                'One of the param does not match the correct timestamp pattern!!')
        else:
            return """
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
                    WHERE schedule_start_time>='2020-10-27 00:00:00' and schedule_status<>'REMOVED'
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
                    WHERE ranked.ranked_num = 1
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
                """.format(time1=completed, time2=newCompleted, time3=onePrior, time4=oneLater)
