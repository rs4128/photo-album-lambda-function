import json
import boto3
from botocore.vendored import requests
import base64


elastic_status = True
url = "https://search-photolabels-rrfomdeoz2rfoebk2erzvznb7q.us-east-1.es.amazonaws.com/photolabels/_search"
headers = {"Content-Type": "application/json"}
auth = requests.auth.HTTPBasicAuth("yt2741", "Acbd_1234")

def get_keywords_lex(event):
    keywords = []
    query = event["q"]
    lex = boto3.client('lex-runtime')
    
    try:
    
        response = lex.post_text(
            botName="PhotoAlbum",
            botAlias="Prod",
            userId="12",
            inputText=query
        )
        
        if "intentName" in response and response["intentName"] == "SearchIntent":
            if response["slots"]["Query"] is not None:
                word = response["slots"]["Query"]
                if word.endswith("s"):
                    word = word[:-1]
                keywords.append(word)
            if response["slots"]["SecondQuery"] is not None:
                word = response["slots"]["SecondQuery"]
                if word.endswith("s"):
                    word = word[:-1]
                keywords.append(word)
    except Exception as e:
        print("Error while retrieving key words from Lex.")
        print("Error: ", str(e))
    
    return keywords

def match_elastic_search(query_key_words, elastic_status=False):
    
    matching_photos = []
    
    if elastic_status==False or query_key_words==[]:
        return matching_photos
    
    try:
        temp_photos = [set(), set()]
        
        for i, key_word in enumerate(query_key_words):
            query = {"query": {
                                  "term": {
                                    "labels": key_word
                                  }
                                }
            }
            
            r = requests.get(url, auth=auth, headers=headers, data=json.dumps(query))
            
            if 'hits' in r.json():
                for result in r.json()['hits']['hits']:
                    temp_photos[i] = temp_photos[i].union(set([result["_source"]["objectKey"]]))
        
        matching_photos = temp_photos[0].intersection(temp_photos[1])
        
    except Exception as e:
        print("Error while retrieving data from elastic search")
        print("Error: ", str(e))
    
    print(matching_photos)
    
    return list(matching_photos)

def get_image_urls(photos):
    
    client = boto3.client('s3')
    
    urls = []
    
    for img in photos:
        response = client.generate_presigned_url('get_object',
                                                Params={'Bucket': "photos-assignment-b2",
                                                        'Key': img})
        urls.append(response)
    
    return urls
    

def lambda_handler(event, context):
    
    query_key_words = get_keywords_lex(event)
    print("Keywords: ", query_key_words)
    
    matching_photos = match_elastic_search(query_key_words, elastic_status)
    
    image_urls = get_image_urls(matching_photos)
    
    #image_urls = get_image_urls(['pisa.jpeg', 'cm.jpeg'])
    
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!'),
        'photos': image_urls
    }
