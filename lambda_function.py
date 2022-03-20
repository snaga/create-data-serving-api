#!/usr/bin/env python3

from yaml import CLoader as Loader, CDumper as Dumper
import boto3
import decimal
import json
import yaml

# Import from the Lambda Layer
from sflib import sflib


ssm = None

def get_ssm_parameter(name):
    global ssm
    if ssm is None:
        ssm = boto3.client('ssm')
    return ssm.get_parameter(Name=name, WithDecryption=True)['Parameter']['Value']
    
def get_api_params(event):
    if event is None: 
       return {}
    params = {}
    if event.get('queryStringParameters'):
        params.update(event.get('queryStringParameters'))
    if event.get('pathParameters'):
        params.update(event.get('pathParameters', {}))
    return params
    

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        return super(DecimalEncoder, self).default(o)


def lambda_handler(event=None, context=None):
    # Get API parameters from query string params and path params
    params = get_api_params(event)
    
    # Get connection params from SSM Parameter Store
    u = get_ssm_parameter('snowflake_username')
    p = get_ssm_parameter('snowflake_password')
    a = get_ssm_parameter('snowflake_account')
    d = get_ssm_parameter('snowflake_database')

    ctx = sflib.connect(u,p,a,d)

    # Get a Snowflake query from an external yaml file.
    query = None
    with open('api.yaml') as f:
        api = yaml.load(f.read(), Loader=Loader)
        if api['type'] == 'snowflake':
            query = api['snowflake']['query']
        f.close()

    # Bind API params to the Snowflake query
    q = sflib.bind_params(query, params)
    
    # Execute query and convert results into a dict array.
    try:
        rs = sflib.query(ctx, q)
    except Exception as ex:
        return {
            'statusCode': 500,
            'body': json.dumps({'params': params,
                                'error': str(ex)}),
            'isBase64Encoded': False
        }

    sflib.close(ctx)

    # Return a response for API Gateway & Lambda integration.
    return {
        'statusCode': 200,
        'headers': {
        },
        'body': json.dumps(rs, cls=DecimalEncoder),
        'isBase64Encoded': False
    }


if __name__ == '__main__':
    event = {}
    event['pathParameters'] = {'custkey': 1}
    print(lambda_handler(event))
