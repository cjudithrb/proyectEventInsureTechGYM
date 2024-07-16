import json
import boto3

def lambda_handler(event, context):
    reclamo = json.dumps(event)

    # Create SQS client
    sqs = boto3.client('sqs')
    queue_url = 'https://sqs.us-east-1.amazonaws.com/743378524363/sqs-reclamo'

    # Send message to SQS queue
    response = sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=(reclamo)
    )
    
    
    sns_client = boto3.client('sns')
    sns_response = sns_client.publish(
        TopicArn='arn:aws:sns:us-east-1:743378524363:registrar_reclamo_sns',
        Message=reclamo,
        Subject='Registro de reclamo'
    )

    # Salida (json)
    return {
        'statusCode': 200,
        'respuesta': 'Registro de reclamo ingresado',
        'detalle': sns_response
    }