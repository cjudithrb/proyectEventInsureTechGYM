import json
import boto3
from datetime import datetime, timedelta
from decimal import Decimal

def lambda_handler(event, context):
    # Entrada (json)
    body = json.loads(event['Records'][0]['body'])
    poliza = json.loads(body['Message'])
    
    # Generar cronograma de pagos
    fecha_expedicion = datetime.strptime(poliza['poliza_datos']['fecha_expedicion'], '%Y-%m-%d')
    importe_total = poliza['poliza_datos']['importe_total']
    importe_mensual = Decimal(importe_total) / Decimal(12)
    cronograma_pagos = []
    
    for i in range(12):
        fecha_pago = fecha_expedicion + timedelta(days=30 * i)
        cronograma_pagos.append({
            'numero_cuota': i + 1,
            'fecha_vencimiento': fecha_pago.strftime('%Y-%m-%d'),
            'importe': round(importe_mensual, 2)
        })
    
    # Convertir importes a Decimal
    for pago in cronograma_pagos:
        pago['importe'] = Decimal(str(pago['importe']))
    
    resultado = {
        'tenant_id': poliza['tenant_id'],
        'poliza_numero': poliza['poliza_numero'],
        'cronograma_pagos': cronograma_pagos
    }
    
    # Proceso
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('t_paymentschedule')
    response = table.put_item(Item=resultado)
    print(resultado)  # Para logs en Cloud Watch
    
    # Salida (json)
    return {
        'statusCode': 200,
        'response': resultado
    }
