import os
import requests
import logging
from google.cloud import vision
import urllib.parse
import proto
import json

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
    
    if len(response.json()):
        print(f'Already ocred and annotated: {resolvable_object_id}')
        return True
    else:
        return False

def annotate(resolvable_object_id, ocr, gbif_id):
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
        'notes': 'automated ocr from batch of images in dataset'
    }
    response = requests.post(url, headers=headers, json=data)
    print(f'Successfully annotated: {response}')

def main():
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
                if not already_annotated_with_ocr(result['occurrenceID']):

                    img = requests.get(result['media'][0]['identifier'])
                    if img.status_code == 200:
                        ocr_text = gv_ocr(img.content) 
                        annotate(result['occurrenceID'], ocr_text, result['gbifID'])
                    else:
                        print(f"ERROR - File has been removed {result['media'][0]['identifier']}")
    else:
        logging.error(f'Bad response {response.status_code} from the GBIF API')

if __name__ == '__main__':
    main()
