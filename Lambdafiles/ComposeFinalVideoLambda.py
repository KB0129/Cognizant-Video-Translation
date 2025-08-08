"""
This AWS Lambda function composes the final dubbed video using ffmpeg.
Downloads the original input video and the merged Japanese MP3 audio from S3.
Merges the video and audio tracks using FFmpeg while preserving video duration and quality.
Uploads the final composed video to S3 under the 'Final/' prefix for downstream use.
"""
import os
import subprocess
import json
import boto3

# Import necessary utility functions from common_utils.py
from common_utils import get_bucket_names, download_file_from_s3, upload_file_to_s3

def lambda_handler(event, context):
    s3 = boto3.client('s3')

    buckets = get_bucket_names()
    output_bucket = buckets['output_bucket']
    input_bucket = buckets['input_bucket'] # Assuming input_bucket is where original videos are located

    input_video_key = event.get('input_video_key')
    # The key passed from GenerateJapaneseAudioLambda is now 'merged_audio_key'
    merged_japanese_audio_key = event.get('merged_audio_key') 

    if not input_video_key:
        raise ValueError("input_video_key is missing in the event payload.")
    if not merged_japanese_audio_key: # Error if the single merged audio file key is missing
        raise ValueError("Merged Japanese audio key is missing in the event payload.")

    # Define local paths for input video, merged audio, and final output video
    base_name = os.path.splitext(os.path.basename(input_video_key))[0]
    local_input_video_path = f'/tmp/{os.path.basename(input_video_key)}'
    local_merged_japanese_audio_path = f'/tmp/{os.path.basename(merged_japanese_audio_key)}' # Path for the single audio file
    final_output_video_key = f"FinalVideos/{base_name}_ja.mp4"
    local_output_video_path = f'/tmp/{os.path.basename(final_output_video_key)}'

    # Download input video and merged Japanese audio from S3
    print(f"Downloading input video from {input_bucket}/{input_video_key}...")
    download_file_from_s3(input_bucket, input_video_key, local_input_video_path)
    print(f"Downloading merged Japanese audio from {output_bucket}/{merged_japanese_audio_key}...")
    download_file_from_s3(output_bucket, merged_japanese_audio_key, local_merged_japanese_audio_path)

    # Build the FFmpeg command
    # This command copies the video stream from the original video
    # and replaces its audio stream with the audio stream from the new merged Japanese audio file.
    ffmpeg_command = [
        'ffmpeg',
        '-i', local_input_video_path,         # First input: original video
        '-i', local_merged_japanese_audio_path, # Second input: merged Japanese audio
        '-map', '0:v:0',                      # Map video stream from the first input (video)
        '-map', '1:a:0',                      # Map audio stream from the second input (audio)
        '-c:v', 'copy',                       # Copy video stream without re-encoding (faster, no quality loss)
        '-c:a', 'aac',                        # Re-encode audio to AAC format
        '-b:a', '192k',                       # Set audio bitrate
        # '-shortest',                          # Terminate encoding when the shortest input stream ends.
                                              # Since GenerateJapaneseAudioLambda already padded the audio to video length,
                                              # this option only affects if the video is shorter than the audio.
                                              # It acts as a safety measure for cases where audio might somehow be longer than video.
        '-y',                                 # Overwrite output file without asking if it already exists.
        local_output_video_path               # Path for the final output video file
    ]

    print(f"Running FFmpeg command: {' '.join(ffmpeg_command)}")

    try:
        process = subprocess.run(ffmpeg_command, check=True, capture_output=True, text=True)
        print("FFmpeg command executed successfully.")
        print("FFmpeg stdout:", process.stdout)
        print("FFmpeg stderr:", process.stderr)
    except subprocess.CalledProcessError as e:
        print(f"FFmpeg error: {e.stderr}")
        raise
    except FileNotFoundError:
        print("FFmpeg not found. Ensure it is installed and in the PATH.")
        raise

    # Upload the final video to S3
    print(f"Uploading final video to S3: {output_bucket}/{final_output_video_key}")
    upload_file_to_s3(output_bucket, final_output_video_key, local_output_video_path, content_type='video/mp4')

    # Clean up temporary files
    print("Cleaning up temporary files...")
    if os.path.exists(local_input_video_path):
        os.remove(local_input_video_path)
    if os.path.exists(local_merged_japanese_audio_path):
        os.remove(local_merged_japanese_audio_path)
    if os.path.exists(local_output_video_path):
        os.remove(local_output_video_path)

    return {
        'input_video_key': input_video_key,
        'final_video_key': final_output_video_key
    }