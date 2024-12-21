import json
import boto3

def lambda_handler(event, context):
    rekognition = boto3.client('rekognition')

    # Process the event (e.g., SNS message)
    sns_message = json.loads(event['Records'][0]['Sns']['Message'])

    # Assuming your SNS message contains the S3 bucket and object key
    bucket = sns_message['bucket']
    key = sns_message['key']

    try:
        # Start video label detection
        response = rekognition.start_label_detection(
            Video={'S3Object': {'Bucket': bucket, 'Name': key}},
            NotificationChannel={
                'SNSTopicArn': 'arn:aws:sns:ap-south-1:339712747456:notification',  # Replace with actual ARN
                'RoleArn': 'arn:aws:iam::339712747456:role/service-role/video-recogn-function-role-q4dioi9z'  # Replace with actual ARN
            }
        )

        # Return the job ID for tracking
        return {
            'statusCode': 200,
            'body': json.dumps({'jobId': response['JobId']})
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
