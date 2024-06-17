import os
import json

zappa_config = {
    "abi": {
        "slim_handler": "true",
        "app_function": "terracotta.server.app.app",
        "aws_region": os.environ['AWS_REGION'],
        "project_name": "fcx-backend",
        "runtime": "python3.7",
        "s3_bucket": os.environ['OUTPUT_DATA_BUCKET'],
        "timeout_seconds": 30,
        "memory_size": 500,
        "aws_environment_variables": {
            "TC_DRIVER_PATH": f"s3://{os.environ['OUTPUT_DATA_BUCKET']}/{os.environ['OUTPUT_DATA_BUCKET_KEY']}/fieldcampaign/goesrplt/abi_allflights.sqlite",
            "TC_DRIVER_PROVIDER": "sqlite-remote",
            "TC_REPROJECTION_METHOD": "linear",
            "TC_RESAMPLING_METHOD": "average",
            "TC_XRAY_PROFILE": "true"
        },
        "manage_roles": False,
        "role_arn": f"{os.environ['ZAPPA_ROLE_ARN']}",
        "vpc_config": {
            "SubnetIds": [ f"{os.environ['SUBNET_ID']}" ],
            "SecurityGroupIds": [ f"{os.environ['SECURITY_GROUP_ID']}" ]
        },
    }
}

with open('zappa_settings.json', 'w') as writer:
    writer.write(json.dumps(zappa_config, sort_keys=True, indent=4))

print("created zappa_settings.json")