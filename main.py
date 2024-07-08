"""
Create usage report for broken users
"""
import os

import pandas as pd
import boto3


def make_report():
    """Make Report"""
    os.environ.get("")
    client = boto3.client(
        "dynamodb",
        region_name="us-east-1",
        aws_access_key_id=os.getenv("aws_access_key_id"),
        aws_secret_access_key=os.getenv("aws_secret_access_key"),
    )
    total_result = []
    isLastEvaluatedKey = True
    ExclusiveStartKey_val = None
    step = 0
    while isLastEvaluatedKey:
        step = step + 1
        print(step)
        res = None
        if step == 1:
            res = client.query(
                TableName="job",
                IndexName="DateQueryIndex",
                KeyConditionExpression="requester_api_key = :requester_api_key AND creation_yearmonth = :creation_yearmonth",
                ExpressionAttributeValues={
                    ":requester_api_key": {
                        "S": "cqytRWIt.eVM0YAsew6Gus1OxvVl4EMPqYA14Nf2n"
                    },
                    ":creation_yearmonth": {"S": "2024-05-28"},
                },
            )
            total_result = total_result + res["Items"]
            print(len(total_result))
            if "LastEvaluatedKey" in res:
                print(res["LastEvaluatedKey"])
                isLastEvaluatedKey = True
                ExclusiveStartKey_val = res["LastEvaluatedKey"]
            else:
                isLastEvaluatedKey = False
        else:
            if isLastEvaluatedKey:
                res = client.query(
                    TableName="job",
                    IndexName="DateQueryIndex",
                    KeyConditionExpression="requester_api_key = :requester_api_key AND creation_yearmonth = :creation_yearmonth",
                    ExpressionAttributeValues={
                        ":requester_api_key": {
                            "S": "cqytRWIt.eVM0YAsew6Gus1OxvVl4EMPqYA14Nf2n"
                        },
                        ":creation_yearmonth": {"S": "2024-05-28"},
                    },
                    ExclusiveStartKey=ExclusiveStartKey_val,
                )
                total_result = total_result + res["Items"]
                print(len(total_result))
                if "LastEvaluatedKey" in res:
                    print(res["LastEvaluatedKey"])
                    isLastEvaluatedKey = True
                    ExclusiveStartKey_val = res["LastEvaluatedKey"]
                else:
                    isLastEvaluatedKey = False
            else:
                break
    df = pd.DataFrame.from_dict(total_result)
    df.to_csv("jeffl@visitordatainc.com_28_05.csv")


if __name__ == "__main__":
    make_report()
