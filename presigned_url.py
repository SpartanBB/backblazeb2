#!/usr/bin/env python3
"""
Purpose

Shows how to use the AWS SDK for Python (Boto3) with BackblazeB2 service
(Backblaze S3 compatible) to generate presigned URLa that can perform an action
for a limited time with your credentials. Also shows how to use the Requests
package to make a request with the URL.
"""

import argparse
import logging
import boto3
from botocore.exceptions import ClientError
import requests
from os import environ

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)
region = "us-west-002"
endpoint_url = f"https://s3.{region}.backblazeb2.com"

# To know your s3 API Url and region you can use the b2_authorize_account Backblaze API.
# https://www.backblaze.com/b2/docs/b2_authorize_account.html
# export s3ApiUrl=$(curl -s https://api.backblazeb2.com/b2api/v2/b2_authorize_account -u "APPLICATION_KEY_ID:APPLICATION_KEY" |jq -r '.s3ApiUrl')
try:
    logger.debug('Getting s3ApiUrl value')
    environ_endpoint_url = environ.get('s3ApiUrl',
                                       f"https://s3.{region}.backblazeb2.com").split('.')
    if len(environ_endpoint_url) == 4 and environ_endpoint_url[0] == "https://s3" and environ_endpoint_url[2] == "backblazeb2" and environ_endpoint_url[2] == "com":
        # Uncomment in case more regions get supported or want loose s3ApiUrl validation
        # region_split = environ_endpoint_url[1].split('-')
        # if len(region_split) == 3 and region_split[0] in ['us', 'eu'] and region_split[1] in ['east', 'west', 'central'] and int(region_split):
        if environ_endpoint_url[1] in ["us-west-000", "us-west-001", "us-west-002", "eu-central-003"]:
            endpoint_url = '.'.join(environ_endpoint_url)
    logger.debug(f"Using the s3ApiUrl {endpoint_url}.")
except:
    logger.error(f'Bad s3ApiUrl value, using {endpoint_url}')


def generate_presigned_url(s3_client, client_method, method_parameters, expires_in):
    """
    Generate a presigned Backblaze B2 URL that can be used to perform an action.

    :param s3_client: A Boto3 Amazon S3 client using a Backblazeb2 endpoint_url.
    :param client_method: The name of the client method that the URL performs.
    :param method_parameters: The parameters of the specified client method.
    :param expires_in: The number of seconds the presigned URL is valid for.
    :return: The presigned URL.
    """
    try:
        url = s3_client.generate_presigned_url(
            ClientMethod=client_method,
            Params=method_parameters,
            ExpiresIn=expires_in
        )
        logger.info("Got presigned URL: %s", url)
    except ClientError:
        logger.exception(
            "Couldn't get a presigned URL for client method '%s'.", client_method)
        raise
    return url


def usage_demo():
    print('-'*88)
    print("Welcome to the Amazon S3 presigned URL demo.")
    print('-'*88)

    parser = argparse.ArgumentParser()
    parser.add_argument('bucket', help="The name of the bucket.")
    parser.add_argument('key', help="The key of the object.")
    parser.add_argument(
        'action', choices=('get', 'put', 'dryget', 'dryput'), help="The action to perform.")
    parser.add_argument(
        'timeout', help="The lifetime in seconds for the presigned url.", default=1000)
    args = parser.parse_args()

    s3_client = boto3.client('s3', region_name=region, 
                            endpoint_url=endpoint_url)
    client_action = 'get_object' if args.action in ['get', 'dryget'] else 'put_object'
    url = generate_presigned_url(
        s3_client, client_action, {'Bucket': args.bucket, 'Key': args.key}, int(args.timeout))

    print("Using the Requests package to send a request to the URL.")
    response = None
    if args.action == 'get':
        response = requests.get(url)
    elif args.action == 'put':
        print("Putting data to the URL.")
        with open(args.key, 'r') as object_file:
            object_text = object_file.read()
        response = requests.put(url, data=object_text)

    if 'dry' in args.action:
        print(f"The presigned URL is: {url}")
        filename = args.key
        if args.action == 'dryput':
            print("To upload the file you can use")
            print(f"curl -XPUT '{url}' -T {filename} -H \"X-Bz-Content-Sha1: $(sha1sum {filename} |cut -d' '  -f1)\"")
        else:
            print("To download the file you can use")
            print(f"curl -XPUT '{url}' -T {filename} -H \"X-Bz-Content-Sha1: $(sha1sum {filename} |cut -d' '  -f1)\"")
    else:
        print("Got response:")
        print(f"Status: {response.status_code}")
        print(response.text)

        print('-'*88)


if __name__ == '__main__':
    usage_demo()