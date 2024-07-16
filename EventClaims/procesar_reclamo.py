import json
import boto3

def lambda_handler(event, context):
    despachador_id = event.get('despachador_id', 'UNKNOWN')
    # Create SQS client
    sqs = boto3.client('sqs')
    queue_url = 'https://sqs.us-east-1.amazonaws.com/743378524363/sqs-reclamo'
    
    # Receive message from SQS queue
    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=3,
        WaitTimeSeconds=10
    )
    
    print(response)
    
    messages = response.get('Messages', [])
    reclamos = []
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('t_reclamos_procesados')

    for message in messages:
        reclamo = json.loads(message['Body'])
        print(reclamo) 
        reclamos.append(reclamo)
        reclamo['despachador_id'] = despachador_id
        response_dynamodb = table.put_item(Item=reclamo)
        receipt_handle = message['ReceiptHandle']
        # Delete received message from queue
        sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )

    return {
        'statusCode': 200,
        'pedidos_procesados': reclamos,
        'cantidad_reclamos_procesados': len(reclamos)
    }