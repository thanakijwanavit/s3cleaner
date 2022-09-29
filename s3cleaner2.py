from boto3 import client, Session
from botocore.exceptions import ClientError
from datetime import datetime, timezone
import argparse

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    
    parser.add_argument('--delete_after_retention_days', required=False, default=15)
    parser.add_argument('--bucket', required=True)
    parser.add_argument('--prefix', required=False, default="")

    args = parser.parse_args()

    delete_after_retention_days = int(args.delete_after_retention_days)
    bucket = args.bucket
    prefix = args.prefix

    # get current date
    today = datetime.now(timezone.utc)

    try:
        # create a connection to Wasabi
        s3_client = client(
            's3')
    except Exception as e:
        raise e

    # create a paginator for all objects.
    object_response_paginator = s3_client.get_paginator('list_object_versions')
    if len(prefix) > 0:
        operation_parameters = {'Bucket': bucket,
                                'Prefix': prefix}
    else:
        operation_parameters = {'Bucket': bucket}

    # instantiate temp variables.
    delete_list = []
    count_current = 0
    count_non_current = 0

    print("$ Paginating bucket " + bucket)
    for object_response_itr in object_response_paginator.paginate(**operation_parameters):
        for version in object_response_itr['Versions']:
            if version["IsLatest"] is True:
                count_current += 1
            elif version["IsLatest"] is False:
                count_non_current += 1
                delete_list.append({'Key': version['Key'], 'VersionId': version['VersionId']})
            if (today - version['LastModified']).days > delete_after_retention_days:
                delete_list.append({'Key': version['Key'], 'VersionId': version['VersionId']})

    # print objects count
    print("-" * 20)
    print("$ Before deleting objects")
    print("$ current objects: " + str(count_current))
    print("$ non-current objects: " + str(count_non_current))
    print("-" * 20)

    # delete objects 1000 at a time
    print("$ Deleting objects from bucket " + bucket)
    for i in range(0, len(delete_list), 1000):
        response = s3_client.delete_objects(
            Bucket=bucket,
            Delete={
                'Objects': delete_list[i:i + 1000],
                'Quiet': True
            }
        )
        print(response)

    # reset counts
    count_current = 0
    count_non_current = 0

    # paginate and recount
    print("$ Paginating bucket " + bucket)
    for object_response_itr in object_response_paginator.paginate(Bucket=bucket):
        if 'Versions' in object_response_itr:
            for version in object_response_itr['Versions']:
                if version["IsLatest"] is True:
                    count_current += 1
                elif version["IsLatest"] is False:
                    count_non_current += 1

    # print objects count
    print("-" * 20)
    print("$ After deleting objects")
    print("$ current objects: " + str(count_current))
    print("$ non-current objects: " + str(count_non_current))
    print("-" * 20)
    print("$ task complete")
