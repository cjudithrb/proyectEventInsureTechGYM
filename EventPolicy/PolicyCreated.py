import json
import boto3

def lambda_handler(event, context):
    poliza = json.dumps(event)

    
    sns_client = boto3.client('sns')
    sns_response = sns_client.publish(
        TopicArn='arn:aws:sns:us-east-1:894146543162:sns_topic_policy',
        Message=poliza,
        Subject='Registro de poliza'
    )

    # Salida (json)
    return {
        'statusCode': 200,
        'respuesta': 'Registro de poliza ingresado',
        'detalle': sns_response
        
    }
    
