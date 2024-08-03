import boto3
import io
from PIL import Image, ImageFilter

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # S3 이벤트에서 버킷 이름과 파일 키 추출
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_key = event['Records'][0]['s3']['object']['key']

    # 원본 이미지 가져오기
    original_image = get_image_from_s3(bucket_name, file_key)

    # 블러 효과 적용
    blurred_image = apply_blur(original_image)

    # 처리된 이미지를 S3에 저장
    destination_bucket = bucket_name
    destination_key = file_key.replace('input/', 'output/')
    upload_image_to_s3(blurred_image, destination_bucket, destination_key)

    return {
        'statusCode': 200,
        'body': f'Image {file_key} has been blurred and saved to {destination_bucket}/{destination_key}'
    }

def get_image_from_s3(bucket_name, file_key):
    # S3에서 이미지 다운로드
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    image_bytes = response['Body'].read()

    # PIL 이미지 객체로 변환
    image = Image.open(io.BytesIO(image_bytes))
    return image

def apply_blur(image):
    # 블러 효과 적용
    blurred_image = image.filter(ImageFilter.BLUR)
    return blurred_image

def upload_image_to_s3(image, bucket_name, file_key):
    # 이미지를 바이트 스트림으로 변환
    image_bytes = io.BytesIO()
    image.save(image_bytes, format='PNG')
    image_bytes = image_bytes.getvalue()

    # S3에 이미지 업로드
    s3.put_object(Bucket=bucket_name, Key=file_key, Body=image_bytes, ContentType='image/png')
