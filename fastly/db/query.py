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

    def getLastCompletedTime(self, job_name):
        if (job_name == ''):
            raise KeyError('Missing job name')
        else:
            return """
                SELECT next_job_start_time
                FROM reporting_cron_job_logs
                WHERE job_name='{job_name}' and status='DONE'
                ORDER BY cron_end_time DESC;
            """.format(job_name=job_name)

    def initiateLog(self, hashed_id='', startStr='', job_name=''):
        if (hashed_id == ''):
            raise KeyError('Missing hashed_id!')
        elif (startStr == ''):
            raise KeyError('Missing cron start time!')
        elif (job_name == ''):
            raise KeyError('Missing job name')
        elif (len(hashed_id) != 64):
            raise KeyError(
                'hashed_id is not 64 characters long, consider passing wrong param')
        elif (self.regexCheck(r'\w{4}-\w{2}-\w{2}T\w{2}:\w{2}:\w{2}Z', startStr) is None):
            raise KeyError(
                'startStr does not match timestamp pattern, consider passing wrong param')
        else:
            return """
                    INSERT INTO reporting_cron_job_logs (hashed_id, cron_start_time, job_name, status) VALUES ('{hashed_id}','{startStr}','{job_name}','RUNNING')
                """.format(hashed_id=hashed_id, startStr=startStr, job_name=job_name)

    def completedLog(self, hashed_id='', endStr='', completedStr='', job_name=''):
        if (hashed_id == ''):
            raise KeyError('Missing hashed_id!')
        elif (endStr == ''):
            raise KeyError('Missing cron end time!')
        elif (completedStr == ''):
            raise KeyError('Missing next job start time!')
        elif (job_name == ''):
            raise KeyError('Missing job name')
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
                    WHERE job_name='{job_name}' AND hashed_id='{hashed_id}'
                """.format(hashed_id=hashed_id, endStr=endStr, completedStr=completedStr, job_name=job_name)

    def errorLog(self, hashed_id='', error='', job_name=''):
        if (hashed_id == ''):
            raise KeyError('Missing hashed_id!')
        elif (error == ''):
            raise KeyError('Missing error!')
        elif (job_name == ''):
            raise KeyError('Missing job name')
        elif (len(hashed_id) != 64):
            raise KeyError(
                'hashed_id is not 64 characters long, consider passing wrong param')
        else:
            return """
                UPDATE reporting_cron_job_logs
                SET status='ERROR', error_note='{error}'
                WHERE job_name='{job_name}' AND hashed_id='{hashed_id}'
            """.format(hashed_id=hashed_id, error=error.replace("'", "\\'"), job_name=job_name)

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
                channel.channel_title as brand_name,
                schedule.program_start_time as program_start_time,
                schedule.program_end_time as program_end_time,
                schedule.program_title as program_title,
                schedule.video_title as video_title,
                schedule.video_description as video_description,
                schedule.video_start_time as video_start_time,
                schedule.video_end_time as video_end_time,
                ROW_NUMBER() OVER (PARTITION BY external_id, program_start_time, id
                                                ORDER BY video_start_time asc) AS video_ranked,
                schedule.video_feed_channel_id as video_feed_channel_id,
                schedule.external_id as external_id,
                schedule.frequency_id as frequency_id,
                upper(split_part(split_part(logs.distributor,'-',2), '/', 1)) as distributor,
                sum(logs.minutes_watched) as minutes_watched,
                logs.client_request as client_request
                FROM (
                SELECT timestamps, channel_id, distributor, minutes_watched, client_request
                FROM fastly_log_aggregated_metadata
                WHERE timestamps>='{time1}' and timestamps<'{time2}' and status>='200' and status<'400'
                ) as logs
                LEFT JOIN (
                SELECT
                    program.linear_channel_id as channel_id,
                    program.schedule_start_time as program_start_time,
                    program.schedule_end_time as program_end_time,
                    program.linear_program_title as program_title,
                    v.video_title as video_title,
                    v.video_description as video_description,
                    video.start_time as video_start_time,
                    video.end_time as video_end_time,
                    v.video_source_id as external_id,
                    v.video_feed_channel_id as video_feed_channel_id,
                    program.video_id as frequency_id
                FROM (
                    SELECT DISTINCT linear_channel_id, schedule_start_time, schedule_end_time, linear_program_id, linear_program_title, linear_program_description, video_id
                    FROM   cms_linear_schedule
                    WHERE schedule_start_time>='{time3}' and schedule_start_time<'{time4}'
                    ORDER  BY schedule_start_time
                ) as program
                LEFT JOIN (
                    SELECT
                    linear_program_id,
                    instruction_start_time,
                    CASE WHEN DATE_PART(min, instruction_start_time) >= 30 THEN DATE_TRUNC('minutes', DATEADD(min, 1, instruction_start_time)) ELSE DATE_TRUNC('minutes', instruction_start_time) END as start_time,
                    CASE WHEN DATE_PART(min, DATEADD('ms', instruction_duration_ms, instruction_start_time)) >= 30 THEN DATE_TRUNC('minutes', DATEADD(min, 1, DATEADD('ms', instruction_duration_ms, instruction_start_time))) ELSE DATE_TRUNC('minutes', DATEADD('ms', instruction_duration_ms, instruction_start_time)) END as end_time,
                    instruction_reference_asset_id
                    FROM (
                    SELECT instruction_start_time, instruction_reference_asset_id, instruction_duration_ms, instruction_reference_start_ms, linear_program_id,
                            ROW_NUMBER() OVER (PARTITION BY linear_program_id, instruction_reference_asset_id, instruction_start_time
                                            ORDER BY event_time desc) AS ranked_num
                    FROM cms_linear_schedule_master
                    WHERE instruction_type='VIDEO' OR instruction_type='INTERSTITIAL' and instruction_start_time>='{time3}' and instruction_start_time<'{time4}'
                    ORDER BY instruction_start_time
                    ) AS ranked
                    WHERE ranked.ranked_num = 1
                ) as video
                ON program.video_id=video.instruction_reference_asset_id and video.linear_program_id=program.linear_program_id and video.start_time>=program.schedule_start_time and video.end_time<=program.schedule_end_time
                LEFT JOIN video_all_data v
                ON program.video_id=v.video_id
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
                on linear.linear_channel_id=logs.channel_id
                LEFT JOIN (
                    SELECT channel_title, channel_id
                    FROM (
                        SELECT channel_title, channel_id, ROW_NUMBER() OVER (PARTITION BY channel_id
                                                ORDER BY processed_time desc) AS ranked_num
                        FROM channel_all_data
                        WHERE event_type='CREATE'
                        ORDER BY processed_time
                    ) as ranked
                    WHERE ranked.ranked_num = 1
                ) as channel
                on channel.channel_id = schedule.video_feed_channel_id
                GROUP BY id, channel_name, brand_name, program_start_time, program_end_time, program_title, video_title, video_description, video_start_time, video_end_time, video_feed_channel_id, external_id, frequency_id, distributor, client_request
                """.format(time1=completed, time2=newCompleted, time3=onePrior, time4=oneLater)

    def checkScheduleID(self, schedule_id=''):
        if (schedule_id == ''):
            raise KeyError('Missing schedule_id!')
        else:
            return """
                SELECT count(*)
                FROM cms_linear_schedule
                WHERE linear_schedule_id='{schedule_id}';
            """.format(schedule_id=schedule_id)

    def fastlyLogWithLinearScheduleQuery(self, completed='', newCompleted='', onePrior='', oneLater=''):
        if (completed == '' or newCompleted == '' or onePrior == ''):
            raise KeyError('Missing one of the param!!')
        elif (None in self.groupRegexCheck(r'\w{4}-\w{2}-\w{2}\s{1}\w{2}:\w{2}:\w{2}', [completed, newCompleted, onePrior])):
            raise KeyError(
                'One of the param does not match the correct timestamp pattern!!')
        else:
            return """
                SELECT
                    timestamps, distributor, city, country, region, continent,
                    CASE WHEN sum(minutes_watched) IS NULL THEN 0 ELSE sum(minutes_watched) END as minutes_watched,
                    CASE WHEN sum(channel_start) IS NULL THEN 0 ELSE sum(channel_start) END as channel_start,
                    CASE WHEN sum(request_size_bytes) IS NULL THEN 0 ELSE sum(request_size_bytes) END as request_size_bytes,
                    CASE WHEN sum(request_count) IS NULL THEN 0 ELSE sum(request_count) END as request_count,
                    CASE WHEN sum(count_720p) IS NULL THEN 0 ELSE sum(count_720p) END as count_720p,
                    CASE WHEN sum(count_1080p) IS NULL THEN 0 ELSE sum(count_1080p) END as count_1080p,
                    CASE WHEN sum(between_720p_and_1080p_count) IS NULL THEN 0 ELSE sum(between_720p_and_1080p_count) END as between_720p_and_1080p_count,
                    CASE WHEN sum(under_720p_count) IS NULL THEN 0 ELSE sum(under_720p_count) END as under_720p_count,
                    CASE WHEN sum(over_1080p_count) IS NULL THEN 0 ELSE sum(over_1080p_count) END as over_1080p_count,
                    channel_id, account_id, linear_channel_id, schedule_start_time, schedule_end_time, schedule_duration_ms, linear_program_id, linear_program_title, linear_program_description, channel_name, client_request
                FROM (
                    SELECT
                        DATE_TRUNC('hours', logs.timestamps) as timestamps,
                        logs.distributor as distributor,
                        logs.city as city,
                        logs.country as country,
                        logs.region as region,
                        logs.continent as continent,
                        logs.minutes_watched as minutes_watched,
                        logs.channel_start as channel_start,
                        logs.request_size_bytes as request_size_bytes,
                        logs.request_count as request_count,
                        logs.count_720p as count_720p,
                        logs.count_1080p as count_1080p,
                        logs.between_720p_and_1080p_count as between_720p_and_1080p_count,
                        logs.under_720p_count as under_720p_count,
                        logs.over_1080p_count as over_1080p_count,
                        logs.channel_id as channel_id,
                        schedule.account_id as account_id,
                        schedule.linear_channel_id as linear_channel_id,
                        schedule.schedule_start_time as schedule_start_time,
                        schedule.schedule_end_time as schedule_end_time,
                        schedule.schedule_duration_ms as schedule_duration_ms,
                        schedule.linear_program_id as linear_program_id,
                        schedule.linear_program_title as linear_program_title,
                        schedule.linear_program_description as linear_program_description,
                        CASE WHEN channel.title IS NULL THEN 'Unknown' ELSE channel.title END as channel_name,
                        logs.client_request as client_request
                    FROM (
                        SELECT
                            timestamps,
                            distributor,
                            CASE WHEN country='US' THEN city ELSE '' END as city,
                            country,
                            region,
                            continent,
                            sum(minutes_watched) as minutes_watched,
                            sum(channel_start) as channel_start,
                            sum(request_size_bytes) as request_size_bytes,
                            sum(request_count) as request_count,
                            sum(count_720p) as count_720p,
                            sum(count_1080p) as count_1080p,
                            sum(between_720p_and_1080p_count) as between_720p_and_1080p_count,
                            sum(under_720p_count) as under_720p_count,
                            sum(over_1080p_count) as over_1080p_count,
                            channel_id,
                            client_request
                        FROM fastly_log_aggregated_metadata
                        WHERE timestamps>='{time1}' and timestamps<'{time2}' and status>='200' and status<'400'
                        GROUP BY timestamps, distributor, city, country, region, continent, channel_id, client_request
                    ) as logs
                    LEFT JOIN (
                        WITH ld AS (
                            SELECT linear_channel_id, max("last_modified_date") AS latest
                            FROM cms_linear_channel
                            GROUP BY linear_channel_id
                        )
                        SELECT linear.linear_channel_id, linear.title
                        FROM cms_linear_channel AS linear
                        JOIN ld ON ld.linear_channel_id = linear.linear_channel_id
                        WHERE linear."last_modified_date" = ld.latest
                    ) as channel
                    ON channel.linear_channel_id=logs.channel_id
                    FULL OUTER JOIN (
                        SELECT DISTINCT account_id, linear_channel_id, schedule_start_time, schedule_end_time, schedule_duration_ms, linear_program_id, linear_program_title, linear_program_description
                        FROM   cms_linear_schedule
                        WHERE schedule_start_time>='{time3}' and schedule_start_time<'{time4}'
                        ORDER  BY schedule_start_time
                    ) as schedule
                    ON logs.timestamps >= schedule.schedule_start_time and logs.timestamps < schedule.schedule_end_time and logs.channel_id=schedule.linear_channel_id
                )
                GROUP BY timestamps, distributor, city, country, region, continent, channel_id, account_id, linear_channel_id, schedule_start_time, schedule_end_time, schedule_duration_ms, linear_program_id, linear_program_title, linear_program_description, channel_name, client_request
                """.format(time1=completed, time2=newCompleted, time3=onePrior, time4=oneLater)

    def fastlyLogWithLinearAndFillRateQuery(self, completed='', newCompleted='', onePrior='', oneLater=''):
        if (completed == '' or newCompleted == '' or onePrior == ''):
            raise KeyError('Missing one of the param!!')
        elif (None in self.groupRegexCheck(r'\w{4}-\w{2}-\w{2}\s{1}\w{2}:\w{2}:\w{2}', [completed, newCompleted, onePrior])):
            raise KeyError(
                'One of the param does not match the correct timestamp pattern!!')
        else:
            return """
                SELECT 
                    agg.timestamps as timestamps,
                    schedule.linear_channel_id as channel_id,
                    schedule.schedule_start_time as schedule_start_time,
                    schedule.schedule_end_time as schedule_end_time,
                    schedule.linear_program_title as linear_program_title,
                    sum(agg.minutes_watched) as minutes_watched,
                    schedule.schedule_duration_ms as schedule_duration_ms,
                    schedule.linear_program_description as linear_program_description,
                    agg.channel_name as channel_name,
                    agg.distributor as distributor,
                    sum(agg.filled_duration_sum) as filled_duration_sum,
                    sum(agg.origin_avail_duration_sum) as origin_avail_duration_sum,
                    sum(agg.num_ads_sum) as num_ads_sum,
                    agg.client_request as client_request
                FROM (
                    SELECT 
                        CASE WHEN a.timestamps IS NULL THEN fill.query_date ELSE a.timestamps END as timestamps,
                        CASE WHEN a.id IS NULL THEN fill.channel_id ELSE a.id END as channel_id,
                        CASE WHEN sum(a.minutes_watched) IS NULL THEN 0 ELSE sum(a.minutes_watched) END as minutes_watched,
                        CASE WHEN linear.channel_name IS NULL THEN 'Unknown' ELSE linear.channel_name END as channel_name,
                        CASE WHEN a.distributor='' THEN fill.distributor ELSE a.distributor END as distributor,
                        sum(fill.filled_duration_sum)  as filled_duration_sum,
                        sum(fill.origin_avail_duration_sum) as origin_avail_duration_sum,
                        sum(fill.num_ads_sum) as num_ads_sum,
                        a.client_request as client_request
                    FROM (
                        SELECT 
                            logs.timestamps as timestamps,
                            logs.channel_id as id,
                            logs.distributor as distributor,
                            sum(logs.minutes_watched) as minutes_watched,
                            logs.client_request as client_request,
                            ROW_NUMBER() OVER (PARTITION BY timestamps, id, distributor ORDER BY client_request DESC) as ranked
                        FROM (
                            SELECT timestamps, CASE WHEN channel_id=' ' THEN NULL ELSE channel_id END as channel_id, upper(split_part(split_part(distributor,'-',2), '/', 1)) as distributor, sum(minutes_watched) as minutes_watched, client_request
                            FROM fastly_log_aggregated_metadata
                            WHERE timestamps>='{time1}' and timestamps<'{time2}' and status>='200' and status<'400'
                            GROUP BY timestamps, channel_id, distributor, client_request
                        ) as logs
                        GROUP BY timestamps, channel_id, distributor, client_request
                    ) as a
                    FULL JOIN (
                        SELECT query_date, (CASE WHEN regexp_substr(origin_id, '-(\\\\d+)-', 1, 1, 'e')='' THEN NULL ELSE regexp_substr(origin_id, '-(\\\\d+)-', 1, 1, 'e') END)::varchar as channel_id, split_part(origin_id,'-',4) as distributor, filled_duration_sum, origin_avail_duration_sum, num_ads_sum
                        FROM cwl_mediatailor_fillrate
                        WHERE query_date>='{time1}' and query_date<'{time2}'
                    ) as fill
                    ON fill.query_date = a.timestamps and fill.channel_id=a.id and fill.distributor=a.distributor and a.ranked=1
                    LEFT JOIN (
                        WITH ld AS (
                            SELECT linear_channel_id::varchar, max("last_modified_date") AS latest 
                            FROM cms_linear_channel 
                            GROUP BY linear_channel_id
                        )
                        SELECT linear.linear_channel_id::varchar, linear.title as channel_name
                        FROM cms_linear_channel AS linear
                        JOIN ld ON ld.linear_channel_id = linear.linear_channel_id
                        WHERE linear."last_modified_date" = ld.latest
                    ) as linear
                    on linear.linear_channel_id=a.id
                    GROUP BY timestamps, fill.query_date, a.id, fill.channel_id, channel_name, a.distributor, fill.distributor, a.client_request
                ) as agg
                LEFT JOIN (
                    SELECT DISTINCT linear_channel_id::varchar, schedule_start_time, schedule_end_time, schedule_duration_ms, linear_program_title, linear_program_description
                    FROM   cms_linear_schedule
                    WHERE schedule_start_time>='{time3}' and schedule_end_time<'{time4}'
                    ORDER  BY schedule_start_time
                ) as schedule
                ON agg.timestamps >= schedule.schedule_start_time and agg.timestamps < schedule.schedule_end_time and agg.channel_id=schedule.linear_channel_id
                GROUP BY timestamps, schedule.linear_channel_id, schedule_start_time, schedule_end_time, linear_program_title, channel_name, distributor, schedule_duration_ms, linear_program_description, client_request
                """.format(time1=completed, time2=newCompleted, time3=onePrior, time4=oneLater)

    def totalBandwidth(self, startStr='', endStr=''):
        if (startStr == ''):
            raise KeyError('Missing query start time!')
        elif (endStr == ''):
            raise KeyError('Missing query end time!')
        elif (None in self.groupRegexCheck(r'\w{4}-\w{2}-\w{2} \w{2}:\w{2}:\w{2}', [startStr, endStr])):
            raise KeyError(
                'Start time or end time does not match timestamp pattern, consider passing wrong param')
        else:
            return """
                    SELECT sum(request_size_bytes) as total
                    FROM fastly_log_aggregated_metadata
                    WHERE timestamps>='{startStr}' and timestamps<'{endStr}'
                """.format(startStr=startStr, endStr=endStr)
