"""
This AWS Lambda function translates English transcription results into Japanese subtitles and stores them in S3.

- Retrieves S3 bucket names using a utility function from common_utils.
- Receives the S3 key of the transcription result and the input video key via the event payload.
- Downloads the transcription result JSON from S3.
- Translates each English subtitle segment to Japanese using Amazon Translate.
- Saves the translated subtitles as a JSON file in the output S3 bucket under the 'TranslateSubtitles/' prefix.
- Returns the input video key and the S3 key for the translated subtitles.
"""
import json
import boto3
import os
from common_utils import get_bucket_names, download_file_from_s3, upload_file_to_s3

def filter_low_confidence_items_and_segments(data, threshold=0.25):
    items = data["results"]["items"]
    segments = data["results"]["audio_segments"]

    # Step 1: 低confidenceの item id を集める
    low_confidence_ids = set()
    for item in items:
        if item["type"] == "pronunciation":
            confidence = float(item["alternatives"][0].get("confidence", "1.0"))
            if confidence < threshold:
                low_confidence_ids.add(item["id"])

    # Step 2: audio_segments を処理して、transcriptとitemsを再構成
    cleaned_segments = []
    for seg in segments:
        new_items = []
        new_transcript_words = []

        for item_id in seg["items"]:
            if item_id not in low_confidence_ids:
                matched_item = next((i for i in items if i["id"] == item_id), None)
                if matched_item:
                    new_items.append(item_id)
                    new_transcript_words.append(matched_item["alternatives"][0]["content"])

        new_transcript = " ".join(new_transcript_words)

        # ✅ transcriptが空欄でない場合のみ追加
        if new_transcript.strip():
            seg["items"] = new_items
            seg["transcript"] = new_transcript
            cleaned_segments.append(seg)

    # 上書き
    data["results"]["audio_segments"] = cleaned_segments
    return data


def lambda_handler(event, context):
    print("[DEBUG] Lambda invoked")
    print("[DEBUG] event =")
    print(json.dumps(event, indent=2))

    bedrock_runtime = boto3.client('bedrock-runtime')

    buckets = get_bucket_names() # Get bucket names from common_utils
    output_bucket = buckets['output_bucket']

    transcribe_result_key = event.get('transcribe_result_key')
    input_video_key = event.get('input_video_key')

    local_transcript_file = '/tmp/transcript.json'
    
    # Download transcribe result from S3
    download_file_from_s3(output_bucket, transcribe_result_key, local_transcript_file)
    
    with open(local_transcript_file, 'r', encoding='utf-8') as f:
        transcript_data = json.load(f)

    transcript_data = filter_low_confidence_items_and_segments(transcript_data, threshold=0.25)
    
    # Prioritize 'audio_segments' for coherent sentences suitable for translation.
    # Fallback to 'items' if 'audio_segments' is not available, but be aware it might contain
    # word-level data that could lead to less natural translations without further processing.
    if 'audio_segments' in transcript_data['results']:
        segments_to_translate = transcript_data['results']['audio_segments']
    elif 'items' in transcript_data['results']:
        segments_to_translate = transcript_data['results']['audio']
        print("Warning: 'audio_segments' not found. Using 'items' instead.")
    else:
        raise ValueError("No valid segments found in transcribe results.")

    subtitles = []
        
    # Translation logic
    for item in segments_to_translate:
        start_time = float(item.get('start_time', 0))
        end_time = float(item.get('end_time', 0))
        max_translated_characters = int((end_time - start_time) * 5.68)

        # Determine the English text based on the structure of the 'item'
        # If from 'audio_segments', it's 'transcript'
        # If from 'items' (word-level), it's 'alternatives'[0]['content']
        if 'transcript' in item:
            english_text = item['transcript']
        elif 'alternatives' in item and item['alternatives']:
            english_text = item['alternatives'][0]['content']
        else:
            print(f"Skipping item due to missing text content: {item}")
            continue # Skip this item if no text can be found
        

        # Construct the prompt for Bedrock for natural translation
        prompt = f"""
        You are a professional translator converting English speech into Japanese subtitles for TTS (text-to-speech) narration.
        Translate the English text into polite, clear, and natural Japanese using the "です・ます" form, suitable for subtitle narration. The style must be concise yet formal and sound natural to native Japanese speakers.

        ⚠️ Your translation must fit within **{max_translated_characters} Japanese characters**. This is a strict limit. Do not exceed it.
        ⚠️ However, DO NOT cut off the translation unnaturally. If your generated sentence becomes too long, **rephrase it naturally to reduce length** while maintaining meaning and politeness. Ensure that the result is a grammatically complete and natural Japanese sentence.

        Omit filler words like "um", "uh", "you know", and similar expressions. If the input consists only of such words, return an empty string without any explanation.
        If the English text consists only of such filler words, return an empty string with absolutely no explanation, placeholder, or substitute text. Just return nothing.

        
        When translating:
        - Use proper particles (助詞) to ensure grammatical clarity and natural flow. Do not omit necessary particles like 「が」「を」「に」「と」.
        - You MAY lightly adjust the sentence structure to improve fluency, only if the meaning is preserved.
        - You MAY add auxiliary words or omit minor details to keep the output natural and concise.
        - If the English text contains the word "Cognizant", do not translate it. Keep "Cognizant" as-is, since it is a proper noun referring to a company name.

        Return only the final translated Japanese text. Do not include line breaks, formatting, or any commentary.

        English text:
        {english_text}
        """

        # print(f"[DEBUG] prompt length: {len(prompt)}")
        # print(f"[DEBUG] prompt preview: {prompt[:500]}...")  

        # Improvement 2: Adjust maxTokenCount for Bedrock model.
        bedrock_max_tokens = 2500 # Increased from 1000 to better accommodate 3000 characters.

        # Invoke Bedrock Claude
        try:
            body = json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "messages": [
                    {
                        "role": "user",
                        "content": prompt.strip()
                    }
                ],
                "max_tokens": bedrock_max_tokens,
                "temperature": 0
            })
            print("[DEBUG] Request body for invoke_model:")
            print(body)

            response = bedrock_runtime.invoke_model(
                body=body,
                modelId="anthropic.claude-3-sonnet-20240229-v1:0",
                accept="application/json",
                contentType="application/json"
            )
            print("[DEBUG] Raw response:")
            print(response)


            response_body = json.loads(response.get('body').read().decode('utf-8'))
            print("[DEBUG] Parsed response body:")
            print(json.dumps(response_body, indent=2))

            if "error" in response_body:
                raise RuntimeError(f"Claude returned error: {response_body['error'].get('message', 'No message')}")

            # Claude 3 expected structure
            content_blocks = response_body.get('content', [])
            if not content_blocks or 'text' not in content_blocks[0]:
                raise ValueError(f"Unexpected response structure: {json.dumps(response_body, indent=2)}")

            translated_text = content_blocks[0]['text'].strip()

            if translated_text == "":
                translated_text = "　"

        except Exception as e:
            import traceback
            print(f"[ERROR] Exception during translation of: {repr(english_text)}")
            print(f"[ERROR] Bedrock request body: {body}")
            traceback.print_exc()
            translated_text = f"[TRANSLATION_ERROR] {english_text}"

            if english_text.strip().lower() in ["um", "uh", "so", "well", "you know", "i mean", "uh,", "um,", "so,", "well,", "you know,", "i mean,"]:
                translated_text = "　"
            else:
                translated_text = f"[TRANSLATION_ERROR] {english_text}"

        subtitles.append({
            'start_time': start_time,
            'end_time': end_time,
            'text': english_text,
            'ja_text': translated_text or ""
        })

    # Save translated subtitles to S3
    translated_subtitle_key = f"TranslateSubtitles/{os.path.basename(transcribe_result_key).replace('.json', '_ja.json')}"
    local_translated_subtitle_file = '/tmp/translated_subtitle.json'
    
    with open(local_translated_subtitle_file, 'w', encoding='utf-8') as f:
        json.dump(subtitles, f, ensure_ascii=False, indent=2)
        
    upload_file_to_s3(output_bucket, translated_subtitle_key, local_translated_subtitle_file, content_type='application/json')

    # Clean up temporary files
    os.remove(local_transcript_file)
    os.remove(local_translated_subtitle_file)

    return {
        'input_video_key': input_video_key,
        'translated_subtitle_key': translated_subtitle_key
    }