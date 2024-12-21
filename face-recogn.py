import boto3
import json
import os

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    rekognition = boto3.client('rekognition')
    
    # Get the uploaded S3 file info
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    # Log the bucket and key to verify input
    print(f"Received event: {json.dumps(event)}")
    print(f"Bucket: {bucket}, Key: {key}")
    
    try:
        # Check the file extension
        file_extension = os.path.splitext(key)[1].lower()
        
        if file_extension in ['.mp4', '.mov', '.avi']:  # Add other video formats as needed
            # Start label detection for video
            response = rekognition.start_label_detection(
                Video={'S3Object': {'Bucket': bucket, 'Name': key}},
                NotificationChannel={
                    'SNSTopicArn': 'arn:aws:sns:your-region:your-account-id:your-sns-topic',  # Update with your SNS topic ARN
                    'RoleArn': 'arn:aws:iam::your-account-id:role/service-role/your-role-name'  # Update with your IAM role ARN
                },
                MinConfidence=75
            )
            print("Started label detection for video. Job ID:", response['JobId'])
        elif file_extension in ['.jpg', '.jpeg', '.png']:  # Add other image formats as needed
            # Call Rekognition to detect labels in the image
            response = rekognition.detect_labels(
                Image={'S3Object': {'Bucket': bucket, 'Name': key}},
                MaxLabels=10
            )
            print("Rekognition response for image:", json.dumps(response))
        else:
            print("Unsupported file type.")
    
    except Exception as e:
        # Log the error if Rekognition fails
        print(f"Error calling Rekognition: {str(e)}")
    
    return {
        'statusCode': 200,
        'body': json.dumps('Rekognition analysis complete')
    }
