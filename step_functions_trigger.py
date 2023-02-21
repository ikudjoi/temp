import boto3
import botocore
import time


class StepFunctionTriggerUtil:
    def __init__(self):
        self.step_function_client = boto3.client("stepfunctions")

        dynamodb_resource = boto3.resource("dynamodb")
        self.lock_table = dynamodb_resource.Table("minerva-step-functions-trigger-lock-table")
        self.queue_table = dynamodb_resource.Table("minerva-step-functions-trigger-queue-table")

    def is_flow_running(self, sfn_arn):
        try:
            response = self.step_function_client.list_executions(stateMachineArn=sfn_arn,
                                                                 statusFilter='RUNNING')
        except self.step_function_client.exceptions.StateMachineDoesNotExist:
            print(f"Tried to list executions of step function '{sfn_arn}' but it doesn't exist, maybe removed?")
            return False

        if response.get('executions', None):
            print(f"Step function '{sfn_arn}' already running.")
            return True

        return False

    @staticmethod
    def try_add_to_dynamodb(table, sfn_arn, ttl_in_seconds):
        ttl = int(time.time()) + ttl_in_seconds
        try:
            table.put_item(
                Item={
                    'sfn_arn': sfn_arn,
                    'ttl': ttl
                },
                ConditionExpression='attribute_not_exists(sfn_arn)'
            )
        except botocore.exceptions.ClientError as e:
            # Ignore the ConditionalCheckFailedException, bubble up
            # other exceptions.
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                return False
            raise

        return True

    def queue_execution(self, sfn_arn):
        # Set time to live to six hours
        self.try_add_to_dynamodb(self.queue_table, sfn_arn, 60*60*6)
        print(f"Queued execution for step function '{sfn_arn}'.")

    def is_locked_by_deploy(self, sfn_arn):
        result = self.lock_table.get_item(Key={'sfn_arn': sfn_arn})
        return result.get('Item', {}).get('locked_by_deploy', False)

    def lock_flow(self, sfn_arn):
        # Set time to live to a minute
        result = self.try_add_to_dynamodb(self.lock_table, sfn_arn, 60)
        if not result:
            if self.is_locked_by_deploy(sfn_arn):
                print("The flow was locked by deployment, queueing instead!")
                self.queue_execution(sfn_arn)
                return False

            print(f"Skipped triggering of step function '{sfn_arn}' "
                  f"as other execution is already doing the same.")
            return False

        print(f"Locked step function '{sfn_arn}' for execution.")
        return True

    def unlock_flow(self, sfn_arn):
        self.lock_table.delete_item(
            Key={'sfn_arn': sfn_arn},
            ConditionExpression='attribute_not_exists(locked_by_deploy)'
        )

    def trigger_execution(self, sfn_arn):
        # If already locked, skip
        if not self.lock_flow(sfn_arn):
            return

        # Double check if concurrent execution already triggered a flow anyway
        if self.is_flow_running(sfn_arn):
            print(f"Was about to execute Step function '{sfn_arn}' but it's already running!")
            self.unlock_flow(sfn_arn)
            return

        try:
            self.step_function_client.start_execution(stateMachineArn=sfn_arn)
            print(f"Triggered execution of step function '{sfn_arn}'.")
        finally:
            self.unlock_flow(sfn_arn)

    def trigger_or_queue_execution(self, sfn_arn):
        if self.is_flow_running(sfn_arn):
            self.queue_execution(sfn_arn)
        else:
            self.trigger_execution(sfn_arn)

    def loop_queue_data(self, db_items):
        for item in db_items:
            sfn_arn = item['sfn_arn']
            if self.is_flow_running(sfn_arn):
                continue

            self.trigger_execution(sfn_arn)
            self.queue_table.delete_item(Key={'sfn_arn': sfn_arn})

    def unload_queue(self):
        response = self.queue_table.scan()
        while True:
            data = response['Items']
            self.loop_queue_data(data)
            if 'LastEvaluatedKey' not in response:
                break

            response = self.queue_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])

    def trigger(self, sfn_arn):
        if sfn_arn:
            self.trigger_or_queue_execution(sfn_arn)
        else:
            print("No SFN function arn given, unloading queue.")
            self.unload_queue()


def get_ssm_parameter(parameter_name):
    ssm_client = boto3.client('ssm')
    return ssm_client.get_parameter(Name=parameter_name)


def get_prefix_to_flow_mapping():
    s3_flows = get_ssm_parameter('etl-flows-triggered-by-s3')
    parameter_value = s3_flows['Parameter']['Value'][5:]  # strip first s3://
    items = parameter_value.split(',s3://')
    return dict([i.split('|') for i in items])


def get_table_to_flow_mapping():
    s3_flows = get_ssm_parameter('etl-flows-triggered-by-snowpipe')
    parameter_value = s3_flows['Parameter']['Value']
    items = parameter_value.split(',')
    return dict([i.split('|') for i in items])


class S3MappingNotFoundError(Exception):
    pass


def s3_event_to_sfn_name(record):
    s3_info = record['s3']
    bucket = s3_info['bucket']['name']
    key = s3_info['object']['key']
    mapping = get_prefix_to_flow_mapping()
    s3_path = f"{bucket}/{key}"
    for m, f in mapping.items():
        if s3_path.startswith(m):
            return f

    raise S3MappingNotFoundError(f"Could not find S3 to flow mapping for S3 key s3://{s3_path}!")


def snowpipe_ingested_notification_to_sfn_name(event):
    sns_attr = event.get('Sns')
    msg_attr = sns_attr.get('MessageAttributes')
    table_attr = msg_attr.get('table')
    table = table_attr.get('Value')
    mapping = get_table_to_flow_mapping()
    sfn_name = mapping.get(table, None)
    if not sfn_name:
        print(f"Table {table} is not mapped to a flow.")
    return sfn_name


def handler(event, context):
    """
    Lambda handler for counting the number of files in a given bucket/prefix and saving the value to a Cloudwatch metric.
    :param event: Event should be either custom event with key 'sfn_name', the name of step function to trigger or queue

        or

        S3 bucket CreateObject event

        or

        Snowpipe monitor Ingested files SNS notification.

    :param context: Lambda context object, used to retrieve account id & region
    :return: None
    """
    print(f"STF trigger function triggered with event {event} and context {context}.")

    sfn_name = None
    records = event.get('Records', None)
    if records:
        record = records[0]
        # Oh why AWS different key casing!!
        if record.get("eventSource", None) == "aws:s3":
            sfn_name = s3_event_to_sfn_name(record)
        elif record.get("EventSource", None) == "aws:sns":
            sfn_name = snowpipe_ingested_notification_to_sfn_name(record)

        # Skip unmapped tables
        if not sfn_name:
            return

    else:
        sfn_name = event.get('sfn_name', None)

    sfn_arn = None
    if sfn_name:
        lambda_arn = context.invoked_function_arn
        region, account_id = lambda_arn.split(":")[3:5]
        sfn_arn = f"arn:aws:states:{region}:{account_id}:stateMachine:{sfn_name}"

    util = StepFunctionTriggerUtil()
    util.trigger(sfn_arn)


# def main():
#     event = {
#         'sfn_name': "forte-booking-state-raw-flow",
#     }
#
#     import types
#     context = types.SimpleNamespace()
#     context.invoked_function_arn = "arn:aws:lambda:eu-west-1:913723161779:function:foobar"
#
#     handler(event, context)


# def main():
#     event = {'Records': [{'eventVersion': '2.1', 'eventSource': 'aws:s3', 'awsRegion': 'eu-west-1',
#                   'eventTime': '2021-08-06T11:48:35.741Z', 'eventName': 'ObjectCreated:Put',
#                   'userIdentity': {'principalId': 'AWS:AROAJOG7Z4SVKUPIX76TU:ilkka.kudjoi@vr.fi'},
#                   'requestParameters': {'sourceIPAddress': '164.5.12.218'},
#                   'responseElements': {'x-amz-request-id': '2RFXAA2QNY3ABFP5',
#                                        'x-amz-id-2': 'wF35RiYmjmodhRLZVMlNSQkmdtnRHLST3cENo1nXJQfABOpQeApuLINeVLXAguFwE6J8putqVkQBlnbuH5bsFef312uPDrv4'},
#                   's3': {'s3SchemaVersion': '1.0', 'configurationId': 'tf-s3-lambda-20210806114721984500000001',
#                          'bucket': {'name': 'minerva-test-integration-in-avecra-navi',
#                                     'ownerIdentity': {'principalId': 'AUJ0BECXL761Y'},
#                                     'arn': 'arn:aws:s3:::minerva-test-integration-in-avecra-navi'},
#                          'object': {'key': 'incoming/perkele', 'size': 0, 'eTag': 'd41d8cd98f00b204e9800998ecf8427e',
#                                     'sequencer': '00610D219A411A354C'}}}]}
#
#     import types
#     context = types.SimpleNamespace()
#     context.invoked_function_arn = "arn:aws:lambda:eu-west-1:913723161779:function:foobar"
#
#     handler(event, context)

#
# def main():
#     event = {'Records': [{'EventSource': 'aws:sns', 'EventVersion': '1.0', 'EventSubscriptionArn': 'arn:aws:sns:eu-west-1:913723161779:minerva-test-snowpipe-ingested-sns:3951acf9-f9ba-492d-9a21-c1ee4edcb366', 'Sns': {'Type': 'Notification', 'MessageId': '8996820d-b703-5a52-ad41-db80c1bbc128', 'TopicArn': 'arn:aws:sns:eu-west-1:913723161779:minerva-test-snowpipe-ingested-sns', 'Subject': 'Snowpipe Monitoring discovered an ingested file.', 'Message': 'File s3://minerva-test-integration-in-wheelq-anonymization/anonymized-data/2021/08/11/4d_p7gv5.json.gz was loaded to table wheelq.api_result.', 'Timestamp': '2021-08-11T06:55:52.973Z', 'SignatureVersion': '1', 'Signature': 'SW2FtKRTDe6pcW44wIQXic9aX/lLkRv9pZqO39xbRH7OVYSr4qOh2Ymc6ux1zSQu/1GvnhaAHcV1PJoZE0dMT15O2BoSoFyqVIU/kCVVb9feDfwxTzojf3uAwHcjeROHF4d1Nksln4gAKCpojbiVxzV3Lgo8jVvUrudRarRhwjQ4MDRWSk1cFkXp+tIikC/Vop3KwUSsPkMizco7HFmW3k5kesIfQaCM+u1+O6cfRqRlhx8qLH+BmSdKjY2slE99lHzUSAb+EzD8jCCzVgPzEYywzCpT74W4cTLvt3LgsteUmttoijDFJH/Bwki27RyiVwb4umU8ifnDfI7AAIMw9A==', 'SigningCertUrl': 'https://sns.eu-west-1.amazonaws.com/SimpleNotificationService-010a507c1833636cd94bdb98bd93083a.pem', 'UnsubscribeUrl': 'https://sns.eu-west-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:eu-west-1:913723161779:minerva-test-snowpipe-ingested-sns:3951acf9-f9ba-492d-9a21-c1ee4edcb366', 'MessageAttributes': {'file': {'Type': 'String', 'Value': 's3://minerva-test-integration-in-wheelq-anonymization/anonymized-data/2021/08/11/4d_p7gv5.json.gz'}, 'source': {'Type': 'String', 'Value': 'Snowpipe Monitoring'}, 'table': {'Type': 'String', 'Value': 'wheelq.api_result'}}}}]}
#
#     import types
#     context = types.SimpleNamespace()
#     context.invoked_function_arn = "arn:aws:lambda:eu-west-1:913723161779:function:foobar"
#
#     handler(event, context)
#
#
# if __name__ == '__main__':
#     main()
