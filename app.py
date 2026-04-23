#!/usr/bin/env python3
import aws_cdk as cdk
from lake_formation_cdk.lake_formation_stack import LakeFormationStack

app = cdk.App()

LakeFormationStack(
    app,
    "LakeFormationPocStack",
    env=cdk.Environment(
        account=app.node.try_get_context("account"),
        region=app.node.try_get_context("region") or "us-east-1",
    ),
    description="AWS Lake Formation + Glue integration PoC",
)

app.synth()
