# Cognizant-Video-Translation
This is the AI system to translate English corporate video into Japanese video on AWS. 

# 🎬 Cognizant Video Translation System

A fully serverless, scalable video translation pipeline built on **AWS**, designed to transcribe, translate, and synthesize multilingual audio from video content. This system enables seamless localization of internal training materials, corporate communications, and media assets.

## 🛠️ Architecture Overview

This solution leverages the following AWS services:

- **AWS Lambda** – Executes lightweight compute tasks for each stage of the pipeline.
- **AWS Step Functions** – Orchestrates the workflow across transcription, translation, and synthesis.
- **Amazon Transcribe** – Converts spoken audio from videos into accurate text.
- **Amazon Bedrock** – Translates transcribed text using foundation models (LLMs).
- **Amazon Polly** – Generates natural-sounding speech from translated text.

## 🔄 Workflow

1. **Video Upload**: A video file is uploaded to an S3 bucket.
2. **Transcription**: Audio is extracted and transcribed using Amazon Transcribe.
3. **Translation**: Transcribed text is translated into the target language via Bedrock.
4. **Voice Synthesis**: Translated text is converted into speech using Amazon Polly.
5. **Output**: The system produces a translated video with voice-over or subtitles.

## 🚀 Features

- Serverless and event-driven architecture
- Scalable and cost-efficient
- Supports multiple languages
- Easy integration with existing AWS environments
- Modular design for future enhancements (e.g., subtitle overlay, speaker diarization)

## 📁 Repository Structure
