import json
import boto3
from botocore.exceptions import BotoCoreError, ClientError
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def format_message(event):
    first_name = event.get('personalInfo', {}).get('firstName', 'Customer')
    insurance_type = event.get('insuranceDetails', {}).get('productName', 'your insurance')
    
    message = (
        f"Hello {first_name},\n\n"
        "Thank you for choosing us for your insurance needs. "
        f"Your request for {insurance_type} has been received and is currently being processed. "
        "We will notify you as soon as there is an update.\n\n"
        "Best regards,\n"
        "Your G & M Brokers Team"
    )
    return message

def lambda_handler(event, context):
    try:
        # Convert event to JSON string
        quotation = json.dumps(event)

        # Create SQS client
        sqs = boto3.client('sqs')
        # Existing queue
        queue_url_1 = 'https://sqs.us-east-1.amazonaws.com/591029114091/sqs-quotes'
        # New queue
        queue_url_2 = 'https://sqs.us-east-1.amazonaws.com/591029114091/sqs-generatepdf'

        # Send message to the existing SQS queue
        response_sqs1 = sqs.send_message(
            QueueUrl=queue_url_1,
            MessageBody=quotation
        )

        # Send message to the new SQS queue
        response_sqs2 = sqs.send_message(
            QueueUrl=queue_url_2,
            MessageBody=quotation
        )

        # Create SNS client and publish message
        sns_client = boto3.client('sns')
        response_sns = sns_client.publish(
            TopicArn='arn:aws:sns:us-east-1:591029114091:ConfirmationEmail',
            Subject='G & M Brokers - Confirmation Email',
            Message=format_message(event)
        )

        # Logging for debugging
        logger.info(f"SNS Response: {response_sns}")
        
        return {
            'statusCode': 200,
            'message': 'Request for quotation successfully completed'
            'response': {'GenerateQuote': response_sqs1, 'GeneratePDF': response_sqs2}
        }
    
    except (BotoCoreError, ClientError) as error:
        logger.error(error)
        return {
            'statusCode': 500,
            'error': str(error)
        }
