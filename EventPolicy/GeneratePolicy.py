import json
import boto3

def lambda_handler(event, context):
    # Entrada (json)
    body = json.loads(event['Records'][0]['body'])
    poliza = json.loads(body['Message'])
    # Proceso
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('t_policy')
    response = table.put_item(Item=poliza)
    print(poliza) # Para logs en Cloud Watch
    # Salida (json)
    return {
        'statusCode': 200,
        'response':poliza
        
    }