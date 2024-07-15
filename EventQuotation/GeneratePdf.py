import json
import boto3
from fpdf import FPDF
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    sqs = boto3.client('sqs')
    queue_url = 'https://sqs.us-east-1.amazonaws.com/591029114091/sqs-generatepdf'
    s3 = boto3.client('s3')
    bucket_name = 'gm-brokers-quotes-storage'
    
    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=10,
        MessageAttributeNames=['All']
    )
    
    messages = response.get('Messages', [])
    if not messages:
        logger.info("No messages received.")
        return {
            'statusCode': 200,
            'body': {
                "message": "No new messages."
            }
        }

    pdf_files = []
    for message in messages:
        quote = json.loads(message['Body'])
        logger.info(f"Processing quote: {quote}")
        pdf_file_name = generate_and_upload_pdf(quote, s3, bucket_name)
        pdf_files.append(pdf_file_name)
        sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=message['ReceiptHandle']
        )

    return {
        'statusCode': 200,
        'body': {
            "message": "PDF generated and uploaded successfully.",
            "pdfFiles": pdf_files
        }
    }
    

def generate_and_upload_pdf(quote, s3, bucket_name):
    logger.info("Starting PDF generation.")
    pdf = FPDF()
    pdf.add_page()

    # Header with logo if available
    # pdf.image('path_to_logo.jpg', 10, 8, 33)  # Adjust path and dimensions
    pdf.set_font("Arial", 'B', 24)
    pdf.cell(0, 20, "G & M Brokers", 0, 1, 'C')

    pdf.set_font("Arial", 'I', 18)
    pdf.cell(0, 10, "Insurance Quotation", 0, 1, 'C')
    pdf.ln(10)  # Add a line break

    # Quotation and Tenant ID
    pdf.set_font("Arial", size=12)
    pdf.cell(0, 10, f"Quotation ID: {quote['quoteId']}", 0, 1)
    pdf.cell(0, 10, f"Broker ID: {quote['tenantId']}", 0, 1)
    pdf.ln(5)

    # Personal Information Section
    pdf.set_fill_color(200, 220, 255)  # Light blue background
    pdf.set_font("Arial", 'B', 14)
    pdf.cell(0, 10, 'Personal Information', 0, 1, fill=True)
    pdf.set_font("Arial", size=12)
    personal_info = quote['personalInfo']
    pdf.cell(0, 10, f"Name: {personal_info['firstName']} {personal_info['lastName']}", 0, 1)
    pdf.cell(0, 10, f"Mother's Last Name: {personal_info['motherLastName']}", 0, 1)
    pdf.cell(0, 10, f"Email: {personal_info['email']}", 0, 1)
    pdf.cell(0, 10, f"Mobile Phone: {personal_info['mobilePhone']}", 0, 1)
    pdf.cell(0, 10, f"Document Type: {personal_info['documentType']}", 0, 1)
    pdf.cell(0, 10, f"Document Number: {personal_info['documentNumber']}", 0, 1)
    pdf.ln(5)

    # Insurance Details Section
    pdf.set_font("Arial", 'B', 14)
    pdf.cell(0, 10, 'Insurance Details', 0, 1, fill=True)
    pdf.set_font("Arial", size=12)
    insurance_details = quote['insuranceDetails']
    pdf.cell(0, 10, f"Product Code: {insurance_details['productCode']}", 0, 1)
    pdf.cell(0, 10, f"Product Name: {insurance_details['productName']}", 0, 1)
    pdf.cell(0, 10, f"Description: {insurance_details['description']}", 0, 1)
    pdf.cell(0, 10, f"Claim History: {'Yes' if insurance_details['hasClaim'] else 'No'}", 0, 1)
    pdf.cell(0, 10, f"Status: {insurance_details['status']}", 0, 1)
    pdf.ln(5)

    # Add more sections as needed...

    pdf_output = '/tmp/quotation.pdf'
    pdf.output(pdf_output)
    
    s3_file_name = f"quotation_{quote['quoteId']}.pdf"
    s3.upload_file(pdf_output, bucket_name, s3_file_name)
    logger.info(f"Successfully uploaded {s3_file_name} to S3")
    
    return s3_file_name