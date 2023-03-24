import os
import requests
import logging
from google.cloud import vision
import urllib.parse
import proto
import json
from minio import Minio

def gv_ocr(content):
    gvclient = vision.ImageAnnotatorClient()
    image = vision.Image(content=content)

    print(f'Attempting to ocr via Google Cloud Vision...')
    response = gvclient.document_text_detection(image=image)
    if response.error.code:
        raise Exception(f"Error from Google Cloud Vision - {response.error}")

    print(f'Successfully ocred')
    texts = proto.Message.to_json(response.full_text_annotation)
    text_json = json.loads(texts)
    return text_json

def already_annotated_with_ocr(resolvable_object_id):
    query_string = f'https://annotater.svc.gbif.no/?source=gcv_ocr_pages&resolvable_object_id={urllib.parse.quote_plus(resolvable_object_id)}'
    response = requests.get(query_string)
    print(f'Annotater query: {query_string}')

    if response.status_code != requests.codes.ok:
        raise Exception
    
    if response.json()['count']:
        print(f'Already ocred and annotated: {resolvable_object_id}')
        return True
    else:
        return False

def annotate(resolvable_object_id, ocr, gbif_id, notes):
    url = 'https://annotater.svc.gbif.no/'
    headers = { 'Authorization': f'Token {os.environ["ANNOTATER_KEY"]}' }
    data = {
        'resolvable_object_id': resolvable_object_id,
        'gbif_id': gbif_id,
        'annotation': ocr['pages'],
        'source': 'gcv_ocr_pages',
        'notes': 'automated ocr from batch of images in dataset'
    }
    response = requests.post(url, headers=headers, json=data)
    data = {
        'resolvable_object_id': resolvable_object_id,
        'gbif_id': gbif_id,
        'annotation': ocr['text'],
        'source': 'gcv_ocr_text',
        'notes': notes
    }
    response = requests.post(url, headers=headers, json=data)
    print(f'Successfully annotated: {response}')

def ocr_and_store(id, image_url, gbif_id=None, notes='automated ocr from batch of images in dataset'):
    if not already_annotated_with_ocr(id):
        img = requests.get(image_url)
        if img.status_code == 200:
            ocr_text = gv_ocr(img.content) 
            annotate(id, ocr_text, gbif_id, notes)
        else:
            print(f"ERROR - File has been removed {image_url}")

def annotate_from_gbif_dataset():
    print(f'Starting... {os.environ["GBIF_IMAGES_DATASETKEY"]}')
    base_url = 'http://api.gbif.org/v1/occurrence/search'
    dataset_key = os.environ['GBIF_IMAGES_DATASETKEY']
    limit = os.environ.get('GBIF_IMAGES_LIMIT', 1)
    offset = os.environ.get('GBIF_IMAGES_LIMIT', 0)
    kingdom = os.environ.get('GBIF_IMAGES_KINGDOM', 'Plantae')
    query_string = f"{base_url}?datasetKey={dataset_key}&limit={limit}&offset={offset}&kingdom={kingdom}&mediaType=StillImage&multimedia=true"

    response = requests.get(query_string)
    if response.status_code == requests.codes.ok:
        results = response.json().get('results')
        print(f'Got {len(results)} from the GBIF API - {query_string}')
        for result in results:
            if 'media' in result:
                ocr_and_store(result['occurrenceID'], result['media'][0]['identifier'], result['gbifID'])
    else:
        logging.error(f'Bad response {response.status_code} from the GBIF API')

def annotate_from_bucket():
    client = Minio(os.getenv('MINIO_URI'), access_key=os.getenv('MINIO_ACCESS_KEY'), secret_key=os.getenv('MINIO_SECRET_KEY'))
    images = client.list_objects(os.getenv('MINIO_BUCKET'), prefix=os.getenv('MINIO_PREFIX'))
    for image in images:
        image_url = f"https://{os.getenv('MINIO_URI')}/{os.getenv('MINIO_BUCKET')}/{image.object_name}"
        ocr_and_store(image_url, image_url, notes="ITALY:Test OCR for Padua")

def main():
    annotate_from_bucket()

if __name__ == '__main__':
    main()
