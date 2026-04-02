"""
Deploy the fn_graph worker as an AWS Lambda container image.

Steps performed:
  1. Load .env
  2. Get AWS account ID
  3. Create ECR repository (if missing)
  4. Authenticate Docker to ECR
  5. Build Lambda Docker image
  6. Tag and push to ECR
  7. Create IAM execution role (if missing)
  8. Create or update Lambda function
  9. Update Lambda config (memory / timeout)

Usage:
    py deploy_lambda.py
"""

import json
import subprocess
import sys
import time
from pathlib import Path

# ── load .env ──────────────────────────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print("[deploy] python-dotenv not installed — relying on environment variables", flush=True)

import boto3
from botocore.exceptions import ClientError

# ── config ─────────────────────────────────────────────────────────────────────
import os

REGION          = os.environ.get("LAMBDA_REGION", os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))
FUNCTION_NAME   = os.environ.get("LAMBDA_FUNCTION_NAME", "fn_graph_worker_lambda")
ECR_REPO_NAME   = "fn-graph-worker-lambda"
ROLE_NAME       = "fn-graph-lambda-execution-role"
MEMORY_MB       = 2048   # scikit-learn + pandas need headroom
TIMEOUT_SECONDS = 300

WORKER_DIR = Path(__file__).parent / "worker"

# ── helpers ────────────────────────────────────────────────────────────────────

def run(cmd: list[str], check=True, extra_env: dict | None = None) -> subprocess.CompletedProcess:
    print(f"[deploy] $ {' '.join(cmd)}", flush=True)
    env = {**os.environ, **(extra_env or {})}
    result = subprocess.run(cmd, capture_output=False, text=True, check=check, env=env)
    return result


def step(msg: str):
    print(f"\n{'='*60}", flush=True)
    print(f"[deploy] {msg}", flush=True)
    print(f"{'='*60}", flush=True)


# ── 1. account ID ──────────────────────────────────────────────────────────────
step("1/8  Fetching AWS account ID")
sts = boto3.client("sts", region_name=REGION)
account_id = sts.get_caller_identity()["Account"]
print(f"[deploy] account: {account_id}  region: {REGION}", flush=True)

ECR_REGISTRY = f"{account_id}.dkr.ecr.{REGION}.amazonaws.com"
ECR_IMAGE_URI = f"{ECR_REGISTRY}/{ECR_REPO_NAME}:latest"

# ── 2. ECR repo ────────────────────────────────────────────────────────────────
step("2/8  Creating ECR repository (if missing)")
ecr = boto3.client("ecr", region_name=REGION)
try:
    ecr.create_repository(repositoryName=ECR_REPO_NAME)
    print(f"[deploy] created ECR repo: {ECR_REPO_NAME}", flush=True)
except ClientError as e:
    if e.response["Error"]["Code"] == "RepositoryAlreadyExistsException":
        print(f"[deploy] ECR repo already exists: {ECR_REPO_NAME}", flush=True)
    else:
        raise

# ── 3. Docker auth to ECR ──────────────────────────────────────────────────────
step("3/8  Authenticating Docker to ECR")
token = ecr.get_authorization_token()
auth_data = token["authorizationData"][0]
import base64 as _b64
user, password = _b64.b64decode(auth_data["authorizationToken"]).decode().split(":", 1)
proc = subprocess.run(
    ["docker", "login", "--username", user, "--password-stdin", ECR_REGISTRY],
    input=password, text=True,
)
if proc.returncode != 0:
    print("[deploy] docker login failed", flush=True)
    sys.exit(1)
print("[deploy] docker login successful", flush=True)

# ── 4. Build image ─────────────────────────────────────────────────────────────
step("4/8  Building Lambda Docker image")
run([
    "docker", "build",
    "--platform", "linux/amd64",
    "-t", ECR_REPO_NAME,
    "-f", str(WORKER_DIR / "Dockerfile.lambda"),
    str(WORKER_DIR),
], extra_env={"DOCKER_BUILDKIT": "0"})

# ── 5. Tag and push ────────────────────────────────────────────────────────────
step("5/8  Tagging and pushing to ECR")
run(["docker", "tag", f"{ECR_REPO_NAME}:latest", ECR_IMAGE_URI])
run(["docker", "push", ECR_IMAGE_URI])

# ── 6. IAM role ────────────────────────────────────────────────────────────────
step("6/8  Creating IAM execution role (if missing)")
iam = boto3.client("iam")
trust_policy = json.dumps({
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Principal": {"Service": "lambda.amazonaws.com"},
        "Action": "sts:AssumeRole",
    }],
})
try:
    role = iam.create_role(
        RoleName=ROLE_NAME,
        AssumeRolePolicyDocument=trust_policy,
        Description="Execution role for fn_graph Lambda worker",
    )
    role_arn = role["Role"]["Arn"]
    print(f"[deploy] created role: {role_arn}", flush=True)
    # Attach basic execution policy
    iam.attach_role_policy(
        RoleName=ROLE_NAME,
        PolicyArn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
    )
    print("[deploy] attached AWSLambdaBasicExecutionRole", flush=True)
    print("[deploy] waiting 10s for IAM role to propagate...", flush=True)
    time.sleep(10)
except ClientError as e:
    if e.response["Error"]["Code"] == "EntityAlreadyExists":
        role_arn = iam.get_role(RoleName=ROLE_NAME)["Role"]["Arn"]
        print(f"[deploy] role already exists: {role_arn}", flush=True)
    else:
        raise

# ── 7. Create or update Lambda ─────────────────────────────────────────────────
step("7/8  Creating or updating Lambda function")
lam = boto3.client("lambda", region_name=REGION)

# Check if function exists and what package type it is
existing_package_type = None
try:
    existing = lam.get_function(FunctionName=FUNCTION_NAME)
    existing_package_type = existing["Configuration"]["PackageType"]
    print(f"[deploy] found existing function, package type: {existing_package_type}", flush=True)
except ClientError as e:
    if e.response["Error"]["Code"] != "ResourceNotFoundException":
        raise

# If it's Zip-based we must delete and recreate — can't switch package types
if existing_package_type == "Zip":
    print(f"[deploy] existing function is Zip-based, deleting so we can recreate as Image...", flush=True)
    lam.delete_function(FunctionName=FUNCTION_NAME)
    print(f"[deploy] deleted {FUNCTION_NAME}, waiting for deletion to propagate...", flush=True)
    time.sleep(5)
    existing_package_type = None

if existing_package_type is None:
    lam.create_function(
        FunctionName=FUNCTION_NAME,
        PackageType="Image",
        Code={"ImageUri": ECR_IMAGE_URI},
        Role=role_arn,
        Timeout=TIMEOUT_SECONDS,
        MemorySize=MEMORY_MB,
        Description="fn_graph pipeline worker — executes serialised node functions",
    )
    print(f"[deploy] Lambda function created: {FUNCTION_NAME}", flush=True)
else:
    # Already Image-based — just update the code
    print(f"[deploy] updating function code...", flush=True)
    lam.update_function_code(
        FunctionName=FUNCTION_NAME,
        ImageUri=ECR_IMAGE_URI,
    )
    print(f"[deploy] waiting for update to complete...", flush=True)
    waiter = lam.get_waiter("function_updated")
    waiter.wait(FunctionName=FUNCTION_NAME)
    lam.update_function_configuration(
        FunctionName=FUNCTION_NAME,
        Timeout=TIMEOUT_SECONDS,
        MemorySize=MEMORY_MB,
    )
    print(f"[deploy] Lambda function updated: {FUNCTION_NAME}", flush=True)

# ── 8. Smoke test ──────────────────────────────────────────────────────────────
step("8/8  Smoke-testing Lambda function")
print("[deploy] waiting for function to become active...", flush=True)
waiter = lam.get_waiter("function_active")
waiter.wait(FunctionName=FUNCTION_NAME)

import cloudpickle, base64 as _b64_2
test_kwargs = {"x": 2, "y": 3}
test_source = "def smoke_test(x, y):\n    return x + y\n"
payload = json.dumps({
    "node_name": "smoke_test",
    "fn_source": test_source,
    "kwargs_b64": _b64_2.b64encode(cloudpickle.dumps(test_kwargs, protocol=4)).decode(),
})
response = lam.invoke(FunctionName=FUNCTION_NAME, Payload=payload.encode())
result_payload = json.loads(response["Payload"].read())
if "error" in result_payload:
    print(f"[deploy] SMOKE TEST FAILED: {result_payload['error']}", flush=True)
    sys.exit(1)

result = cloudpickle.loads(_b64_2.b64decode(result_payload["result_b64"]))
assert result == 5, f"expected 5, got {result}"
print(f"[deploy] smoke test passed: smoke_test(2, 3) = {result}", flush=True)

print(f"""
{'='*60}
[deploy] DONE

  Function : {FUNCTION_NAME}
  Region   : {REGION}
  Image    : {ECR_IMAGE_URI}
  Memory   : {MEMORY_MB} MB
  Timeout  : {TIMEOUT_SECONDS}s

Add to pipeline_config.yaml:
  executor: lambda
  function_name: {FUNCTION_NAME}
  region: {REGION}
{'='*60}
""", flush=True)
