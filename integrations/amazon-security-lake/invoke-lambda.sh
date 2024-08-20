#!/bin/bash

export S3_BUCKET_RAW=wazuh-aws-security-lake-raw

curl -X POST "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{
  "Records": [
    {
      "eventVersion": "2.0",
      "eventSource": "aws:s3",
      "awsRegion": "us-east-1",
      "eventTime": "1970-01-01T00:00:00.000Z",
      "eventName": "ObjectCreated:Put",
      "userIdentity": {
        "principalId": "AIDAJDPLRKLG7UEXAMPLE"
      },
      "requestParameters":{
        "sourceIPAddress":"127.0.0.1"
      },
      "responseElements":{
        "x-amz-request-id":"C3D13FE58DE4C810",
        "x-amz-id-2":"FMyUVURIY8/IgAtTv8xRjskZQpcIZ9KG4V5Wp6S7S/JRWeUWerMUE5JgHvANOjpD"
      },
      "s3": {
        "s3SchemaVersion": "1.0",
        "configurationId": "testConfigRule",
        "bucket": {
          "name": "'"${S3_BUCKET_RAW}"'",
          "ownerIdentity": {
            "principalId":"A3NL1KOZZKExample"
          },
          "arn": "'"arn:aws:s3:::${S3_BUCKET_RAW}"'"
        },
        "object": {
          "key": "'"${1}"'",
          "size": 1024,
          "eTag":"d41d8cd98f00b204e9800998ecf8427e",
          "versionId":"096fKKXTRTtl3on89fVO.nfljtsv6qko"
        }
      }
    }
  ]
}'
