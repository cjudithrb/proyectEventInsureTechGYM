import json
import boto3

def lambda_handler(event, context):
    sqs = boto3.client('sqs')
    queue_url = 'https://sqs.us-east-1.amazonaws.com/591029114091/sqs-quotes'
    
    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=10
    )
    
    messages = response.get('Messages', [])
    quotations = []
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('t_quotations')
    
    for message in messages:
        quote = json.loads(message['Body'])
        required_keys = ['tenantId', 'quoteId', 'personalInfo', 'insuranceDetails']
        if not all(key in quote for key in required_keys):
            print(f"Missing required keys in quote: {quote}")
            continue

        quotations.append(quote)
        response_dynamodb = table.put_item(Item=quote)
        if response_dynamodb['ResponseMetadata']['HTTPStatusCode'] == 200:
            print(f"Successfully inserted quote: {quote['quoteId']} into DynamoDB")
        else:
            print(f"Failed to insert quote: {quote['quoteId']} into DynamoDB")
        
        sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=message['ReceiptHandle']
        )

    return {
        'statusCode': 200,
        'processed_quotes': quotations
    }
