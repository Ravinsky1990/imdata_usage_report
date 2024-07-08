import os

from get_api_clients import get_api_clients
from get_date_usage import get_date_usage
from csv_reader import process_data_dicts

# import pandas as pd
import boto3
from boto3.dynamodb.conditions import Key


def generate_usage_report():
    """Generates usage report for client for previous day"""
    os.environ.get("")
    api_clients = get_api_clients()
    usage_date = get_date_usage()
    dd_client = boto3.client(
        "dynamodb",
        region_name="us-east-1",
        aws_access_key_id=os.getenv("aws_access_key_id"),
        aws_secret_access_key=os.getenv("aws_secret_access_key"),
    )
    for client in api_clients:
        print(f"Fetching usage for ${client['client_email']} by ${usage_date}")
        total_result = []
        isLastEvaluatedKey = True
        ExclusiveStartKey_val = None
        step = 0
        while isLastEvaluatedKey:
            step = step + 1
            res = None
            if step == 1:
                res = dd_client.query(
                    TableName="job",
                    IndexName="DateQueryIndex",
                    KeyConditionExpression="requester_api_key = :requester_api_key AND creation_yearmonth = :creation_yearmonth",
                    ExpressionAttributeValues={
                        ":requester_api_key": {"S": client["api_key"]},
                        ":creation_yearmonth": {"S": usage_date},
                    },
                )
                total_result = total_result + res["Items"]
                if "LastEvaluatedKey" in res:
                    isLastEvaluatedKey = True
                    ExclusiveStartKey_val = res["LastEvaluatedKey"]
                else:
                    isLastEvaluatedKey = False
            else:
                if isLastEvaluatedKey:
                    res = dd_client.query(
                        TableName="job",
                        IndexName="DateQueryIndex",
                        KeyConditionExpression="requester_api_key = :requester_api_key AND creation_yearmonth = :creation_yearmonth",
                        ExpressionAttributeValues={
                            ":requester_api_key": {"S": client["api_key"]},
                            ":creation_yearmonth": {"S": usage_date},
                        },
                        ExclusiveStartKey=ExclusiveStartKey_val,
                    )
                    total_result = total_result + res["Items"]
                    if "LastEvaluatedKey" in res:
                        isLastEvaluatedKey = True
                        ExclusiveStartKey_val = res["LastEvaluatedKey"]
                    else:
                        isLastEvaluatedKey = False
                else:
                    break
        print(f"${client['client_email']} - ${len(total_result)}")
        # Process fetched data from dd and generate report dict
        day_counts = process_data_dicts(total_result)
        


generate_usage_report()
