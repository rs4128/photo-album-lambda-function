import json
import datetime
import boto3
from botocore.vendored import requests

elastic_status = True
url = "https://search-photolabels-rrfomdeoz2rfoebk2erzvznb7q.us-east-1.es.amazonaws.com/photolabels/_doc"
headers = {"Content-Type": "application/json"}
auth = requests.auth.HTTPBasicAuth("yt2741", "Acbd_1234")

def get_image_meta_data(image, bucket):
    #get image meta data and custom labels
    meta_data = {}
    client = boto3.client('s3')
    response = client.get_object(Bucket=bucket,Key=image)

    
    meta_data['objectKey'] = image
    meta_data['bucket'] = bucket
    meta_data['createdTimestamp'] = response["LastModified"].strftime("%m/%d/%Y, %H:%M:%S")
    meta_data['labels'] = response["Metadata"]["customlabels"].split(",")
    
    return meta_data
    
def get_image_labels(image, bucket):
    #call recognition
    labels = []
    
    client = boto3.client('rekognition')
    response = client.detect_labels(Image={'S3Object':{'Bucket':bucket,'Name':image}})
    
    for label in response['Labels']:
        labels.append(label["Name"])
    
    return labels

def create_json(image_data, labels):
    image_data["labels"] += labels
    
    image_data["labels"] = [x.lower() for x in image_data["labels"]]
    
    return image_data

def push_to_open_search(image_data, elastic_status = False):
    
    if elastic_status == True:
        x = requests.post(url, data=json.dumps(image_data), headers=headers, auth=auth)
        print(x.text)
    

def lambda_handler(event, context):
    # TODO implement
    
    print(event)
    
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    image_name = event["Records"][0]["s3"]["object"]["key"]
    
    print(image_name, bucket_name)
    
    image_meta_data = get_image_meta_data(image_name, bucket_name)
    image_labels = get_image_labels(image_name, bucket_name)
    return_json = create_json(image_meta_data, image_labels)
    print(return_json)
    
    #return_json = {'objectKey': 'shutterstock_405118426.jpeg', 'bucket': 'photos-assignment-b2', 'createdTimestamp': datetime.datetime(2022, 3, 16, 21, 6, 9).strftime("%m/%d/%Y, %H:%M:%S"), 'labels': ['Dog', 'Canine', 'Pet', 'Animal', 'Mammal', 'Ground', 'Tree', 'Plant', 'Vegetation', 'Leaf', 'Tree Trunk', 'Outdoors', 'Land', 'Nature', 'Puppy', 'Terrier', 'Hound']}
    
    #push to elastic search
    push_to_open_search(return_json, elastic_status)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
