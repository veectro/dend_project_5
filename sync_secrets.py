import configparser

import boto3
import json
import shutil

AWS_PROFILE = 'udacity'
AWS_REGION = 'us-west-2'
SECRET_NAME = 'udacity_dend_secret'


def get_secret(secret_name, region_name=AWS_REGION, aws_profile=AWS_PROFILE) -> dict:
    """
    Get secret from AWS Secrets Manager
    :param secret_name: secret name in AWS Secrets Manager
    :param region_name: region name
    :param aws_profile: aws profile to used (from ~/.aws/credentials)
    :return: dictionary that contains the secret
    """
    session = boto3.session.Session(profile_name=aws_profile)
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    get_secret_value_response = client.get_secret_value(
        SecretId=secret_name
    )
    if 'SecretString' in get_secret_value_response:
        secret = get_secret_value_response['SecretString']
    else:
        secret = get_secret_value_response['SecretBinary']
    return json.loads(secret)


if __name__ == '__main__':
    # copy file dwh.cfg.template to dwh.cfg with shutil
    shutil.copy('dwh.cfg.template', 'dwh.cfg')

    try:
        secret = get_secret(SECRET_NAME)
        airlow_conn_id = f"postgresql://{secret['REDSHIFT_USERNAME']}:{secret['REDSHIFT_PASSWORD']}" \
                         f"@{secret['REDSHIFT_ENDPOINT'].split(':')[0]}:{secret['REDSHIFT_ENDPOINT'].split(':')[1]}" \
                         f"/{secret['REDSHIFT_DATABASE']}"
        with open('.env', 'w') as f:
            f.write(f'AIRFLOW_CONN_REDSHIFT_CONN_ID={airlow_conn_id}\n')
            f.write(f'AIRFLOW_VAR_REDSHIFT_IAM_ARN={secret["REDSHIFT_ROLE_ARN"]}')

    except Exception as e:
        print(e)
