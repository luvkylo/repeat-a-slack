msg = `
You are receiving this email because your Amazon CloudWatch Alarm "Linear-255-1-EuroNews-ML-ActiveAlerts" in the US West (Oregon) region has entered the OK state, because "Threshold Crossed: 3 out of the last 5 datapoints [0.0 (21/12/21 06:45:00), 0.0 (21/12/21 06:44:00), 0.0 (21/12/21 06:42:00)] were not greater than the threshold (0.0) (minimum 3 datapoints for ALARM -> OK transition)." at "Tuesday 21 December, 2021 06:46:11 UTC". View this alarm in the AWS Management Console: https://us-west-2.console.aws.amazon.com/cloudwatch/deeplink.js?region=us-west-2#alarmsV2:alarm/Linear-255-1-EuroNews-ML-ActiveAlerts Alarm Details: 
- Name: Linear-255-1-EuroNews-ML-ActiveAlerts 
- Description: [ALM-MLACTVAL] Alarm if MediaLive Channel ActiveAlerts > 0 - State Change: ALARM -> OK - Reason for State Change: Threshold Crossed: 3 out of the last 5 datapoints [0.0 (21/12/21 06:45:00), 0.0 (21/12/21 06:44:00), 0.0 (21/12/21 06:42:00)] were not greater than the threshold (0.0) (minimum 3 datapoints for ALARM -> OK transition). 
- Timestamp: Tuesday 21 December, 2021 06:46:11 UTC 
- AWS Account: 881583556644 - Alarm Arn: arn:aws:cloudwatch:us-west-2:881583556644:alarm:Linear-255-1-EuroNews-ML-ActiveAlerts Threshold: - The alarm is in the ALARM state when the metric is GreaterThanThreshold 0.0 for 60 seconds. Monitored Metric: - MetricNamespace: MediaLive - MetricName: ActiveAlerts - Dimensions: [ChannelId = 5524579] [Pipeline = 0] - Period: 60 seconds - Statistic: Sum - Unit: not specified State Change Actions: - OK: [arn:aws:sns:us-west-2:881583556644:MediaLive-Channel-Alarms] - ALARM: [arn:aws:sns:us-west-2:881583556644:MediaLive-Channel-Alarms] - INSUFFICIENT_DATA: -- If you wish to stop receiving notifications from this topic, please click or visit the link below to unsubscribe: https://sns.us-west-2.amazonaws.com/unsubscribe.html?SubscriptionArn=arn:aws:sns:us-west-2:881583556644:MediaLive-Channel-Alarms:2e968e19-3e2d-42fc-8a51-8940a690d86b&Endpoint=alerts-playout-aaaaekqhbbb4gtqoau3t5u432q@frequencynetworks.slack.com Please do not reply directly to this email. If you have any questions or comments regarding this email, please contact us at https://aws.amazon.com/support
`

let found = msg.match(/"Linear-(\d+)-/);
console.log(found[1]);

let name = msg.match(/Name:\s+(?<name>.+)/);
console.log(name.groups.name);