import json
import boto3
from datetime import datetime, timedelta
from fpdf import FPDF
from io import BytesIO

def lambda_handler(event, context):
    # Entrada (json)
    body = json.loads(event['Records'][0]['body'])
    poliza = json.loads(body['Message'])
    
    # Crear PDF con FPDF
    class PDF(FPDF):
        def header(self):
            self.set_font('Arial', 'B', 12)
            self.cell(0, 10, 'Póliza de Seguro', 0, 1, 'C')
    
    pdf = PDF()
    pdf.add_page()
    
    # Información de la póliza
    pdf.set_font('Arial', '', 12)
    pdf.cell(0, 10, f"Tenant ID: {poliza['tenant_id']}", 0, 1)
    pdf.cell(0, 10, f"Poliza Número: {poliza['poliza_numero']}", 0, 1)
    pdf.cell(0, 10, f"Fecha de Expedición: {poliza['poliza_datos']['fecha_expedicion']}", 0, 1)
    pdf.cell(0, 10, f"Vigencia Desde: {poliza['poliza_datos']['vigencia_desde']}", 0, 1)
    pdf.cell(0, 10, f"Vigencia Hasta: {poliza['poliza_datos']['vigencia_hasta']}", 0, 1)
    
    # Información del asegurado
    pdf.set_font('Arial', 'B', 12)
    pdf.cell(0, 10, "Información del Asegurado", 0, 1)
    pdf.set_font('Arial', '', 12)
    pdf.cell(0, 10, f"Nombre Asegurado: {poliza['poliza_datos']['nombre_asegurado']}", 0, 1)
    pdf.cell(0, 10, f"Dirección Asegurado: {poliza['poliza_datos']['direccion_asegurado']}", 0, 1)
    pdf.cell(0, 10, f"RFC Asegurado: {poliza['poliza_datos']['rfc_asegurado']}", 0, 1)
    
    # Información del vehículo
    pdf.set_font('Arial', 'B', 12)
    pdf.cell(0, 10, "Descripción del Vehículo", 0, 1)
    pdf.set_font('Arial', '', 12)
    pdf.cell(0, 10, f"Descripción: {poliza['poliza_datos']['descripcion_vehiculo']}", 0, 1)
    pdf.cell(0, 10, f"Modelo: {poliza['poliza_datos']['modelo_vehiculo']}", 0, 1)
    pdf.cell(0, 10, f"Número de Serie: {poliza['poliza_datos']['numero_serie']}", 0, 1)
    pdf.cell(0, 10, f"Número de Motor: {poliza['poliza_datos']['numero_motor']}", 0, 1)
    
    # Coberturas Contratadas
    pdf.set_font('Arial', 'B', 12)
    pdf.cell(0, 10, "Coberturas Contratadas", 0, 1)
    pdf.set_font('Arial', '', 12)
    for cobertura, detalles in poliza['poliza_datos']['coberturas_contratadas'].items():
        pdf.cell(0, 10, f"{cobertura.replace('_', ' ').title()}:", 0, 1)
        for key, value in detalles.items():
            pdf.cell(0, 10, f"  {key.replace('_', ' ').title()}: {value}", 0, 1)
    
    # Datos de contacto
    pdf.set_font('Arial', 'B', 12)
    pdf.cell(0, 10, "Datos de Contacto", 0, 1)
    pdf.set_font('Arial', '', 12)
    pdf.cell(0, 10, f"Calle: {poliza['direccion_respuesta']['calle']}", 0, 1)
    pdf.cell(0, 10, f"Distrito: {poliza['direccion_respuesta']['distrito']}", 0, 1)
    pdf.cell(0, 10, f"Provincia: {poliza['direccion_respuesta']['provincia']}", 0, 1)
    pdf.cell(0, 10, f"País: {poliza['direccion_respuesta']['pais']}", 0, 1)
    
    # Guardar PDF en buffer
    buffer = BytesIO()
    pdf.output(buffer)
    buffer.seek(0)
    
    # Guardar PDF en S3
    s3 = boto3.client('s3')
    bucket_name = 'gm-broken-policy-storage'
    pdf_name = f"poliza_{poliza['poliza_numero']}.pdf"
    s3.put_object(Bucket=bucket_name, Key=pdf_name, Body=buffer, ContentType='application/pdf')
    
    print(poliza)  # Para logs en Cloud Watch
    
    # Salida (json)
    return {
        'statusCode': 200,
        'response': poliza
    }
