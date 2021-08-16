# lambda-transient-emr-spark
Lambda script to trigger spark job on emr clusters utilising spot instances using glue data catalog as external metastore.

Policy needed for lambda function to access emr roles:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Action": [
                "iam:PassRole"
            ],
            "Resource": [
                "*"
            ],
            "Effect": "Allow"
        }
    ]
}
```
