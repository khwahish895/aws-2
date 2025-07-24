import json
import boto3
import time
import os

s3 = boto3.client('s3')
transcribe = boto3.client('transcribe')
ses = boto3.client('ses', region_name='us-east-1')
bedrock = boto3.client('bedrock-runtime')

def lambda_handler(event, context):
    # Get S3 info
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    s3_uri = f"s3://{bucket}/{key}"
    
    job_name = f"transcribe-{key.replace('.', '-')}-{int(time.time())}"
    output_bucket = bucket
    transcribe_uri = f"https://s3.amazonaws.com/{bucket}/{key}"

    # Start transcription
    transcribe.start_transcription_job(
        TranscriptionJobName=job_name,
        Media={'MediaFileUri': transcribe_uri},
        MediaFormat=key.split('.')[-1],
        LanguageCode='en-US',
        OutputBucketName=output_bucket
    )

    # Wait for job completion
    while True:
        status = transcribe.get_transcription_job(TranscriptionJobName=job_name)
        if status['TranscriptionJob']['TranscriptionJobStatus'] in ['COMPLETED', 'FAILED']:
            break
        time.sleep(10)

    if status['TranscriptionJob']['TranscriptionJobStatus'] == 'COMPLETED':
        transcript_uri = status['TranscriptionJob']['Transcript']['TranscriptFileUri']
        transcript_data = boto3.client('s3').get_object(Bucket=bucket, Key=f"{job_name}.json")
        text_json = json.loads(transcript_data['Body'].read())
        transcript_text = text_json['results']['transcripts'][0]['transcript']
    else:
        transcript_text = "Transcription failed."

    # Use Bedrock to summarize
    bedrock_input = {
        "prompt": f"\n\nHuman: Summarize this transcript:\n{transcript_text}\n\nAssistant:",
        "max_tokens_to_sample": 300
    }

    bedrock_response = bedrock.invoke_model(
        modelId='anthropic.claude-v2',
        body=json.dumps(bedrock_input),
        contentType='application/json',
        accept='application/json'
    )

    summary = json.loads(bedrock_response['body'].read())['completion']

    # Save to S3
    s3.put_object(Bucket=bucket, Key=f"summaries/{key}_summary.txt", Body=summary)
    s3.put_object(Bucket=bucket, Key=f"transcripts/{key}_transcript.txt", Body=transcript_text)

    # Email Notification
    ses.send_email(
        Source=os.environ['SOURCE_EMAIL'],
        Destination={'ToAddresses': [os.environ['DEST_EMAIL']]},
        Message={
            'Subject': {'Data': 'New Video Uploaded'},
            'Body': {
                'Text': {
                    'Data': f"Video uploaded:\nFile: {key}\nS3 URI: {s3_uri}\n\nSummary:\n{summary}"
                }
            }
        }
    )

    return {'statusCode': 200, 'body': 'Email sent with summary.'}
