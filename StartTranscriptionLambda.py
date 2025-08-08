"""
This AWS Lambda function initiates a transcription job for a given input video file stored in S3.

- Retrieves S3 bucket names using a utility function from common_utils.
- Receives the S3 key of the input video via the event payload.
- Starts an Amazon Transcribe job to transcribe the audio from the video (assumed to be in English).
- Stores the transcription result as a JSON file in the output S3 bucket under the 'TranscribeResults/' prefix.
- Returns the input video key, the S3 key for the transcription result, and the transcription job name.
"""
import boto3
import time
import os
from common_utils import get_bucket_names

def filter_low_confidence_items_and_segments(data, threshold=0.25):
    items = data["results"]["items"]
    segments = data["results"]["audio_segments"]

    low_confidence_ids = set()
    for item in items:
        if item["type"] == "pronunciation":
            confidence = float(item["alternatives"][0].get("confidence", "1.0"))
            if confidence < threshold:
                low_confidence_ids.add(item["id"])

    for seg in segments:
        new_items = []
        new_transcript_words = []
        for item_id in seg["items"]:
            if item_id not in low_confidence_ids:
                new_items.append(item_id)
                matched_item = next((i for i in items if i["id"] == item_id), None)
                if matched_item:
                    new_transcript_words.append(matched_item["alternatives"][0]["content"])
        seg["items"] = new_items
        seg["transcript"] = " ".join(new_transcript_words)

    return data

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    transcribe = boto3.client('transcribe')

    buckets = get_bucket_names()    # Get bucket names from common_utils        
    input_bucket = buckets['input_bucket']
    output_bucket = buckets['output_bucket']

    input_video_key = event['input_video_key']

    transcribe_result_key = f"TranscribeResults/{os.path.basename(input_video_key).replace('.mp4', '.json')}"

    timestamp = int(time.time())
    job_name = f"transcribe_job_{timestamp}"

    media_url = f's3://{input_bucket}/{input_video_key}'

    response = transcribe.start_transcription_job(
        TranscriptionJobName=job_name,
        Media={'MediaFileUri': media_url},
        MediaFormat='mp4',
        LanguageCode='en-US',
        OutputBucketName=output_bucket,
        OutputKey=transcribe_result_key
    )

    return {
        'input_video_key': input_video_key,
        'transcribe_result_key': transcribe_result_key,
        'transcription_job_name': job_name
    }