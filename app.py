#!/usr/bin/env python3
import os
import aws_cdk as cdk
from lake_formation_cdk.lake_formation_stack import LakeFormationStack

app = cdk.App()

LakeFormationStack(
    app,
    "LakeFormationPocStack",
    env=cdk.Environment(
        account=app.node.try_get_context("account") or os.environ["CDK_DEFAULT_ACCOUNT"],
        region=app.node.try_get_context("region") or os.environ.get("CDK_DEFAULT_REGION", "us-east-1"),
    ),
    description="AWS Lake Formation + Glue integration PoC",
)

app.synth()
