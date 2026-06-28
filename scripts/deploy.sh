#!/usr/bin/env bash
# ============================================================
# deploy.sh — Deploy or update the CloudFormation stack.
# Run setup.sh first, then: source .env && ./scripts/deploy.sh
# ============================================================
set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; BOLD='\033[1m'; NC='\033[0m'
info()    { echo -e "${BLUE}[INFO]${NC} $*"; }
success() { echo -e "${GREEN}[OK]${NC}  $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC} $*"; }
error()   { echo -e "${RED}[ERR]${NC}  $*"; exit 1; }

# ── Load env ──
[[ -f .env ]] && source .env || warn ".env not found — using shell env"

PROJECT_NAME="${PROJECT_NAME:-ecommerce-pipeline}"
AWS_REGION="${AWS_REGION:-us-east-1}"
AWS_ACCOUNT="${AWS_ACCOUNT:-$(aws sts get-caller-identity --query Account --output text)}"
SCRIPTS_BUCKET="${SCRIPTS_BUCKET:-${PROJECT_NAME}-scripts-${AWS_ACCOUNT}}"
ALERT_EMAIL="${ALERT_EMAIL:-you@example.com}"
STACK_NAME="${PROJECT_NAME}-stack"

echo ""
echo -e "${BOLD}🚀 Deploying E-Commerce Analytics Pipeline${NC}"
echo "  Stack   : $STACK_NAME"
echo "  Region  : $AWS_REGION"
echo ""

# ── Re-upload scripts (always latest) ──
info "Syncing Glue scripts to S3 …"
aws s3 cp src/glue/bronze_to_silver.py \
    "s3://${SCRIPTS_BUCKET}/glue/bronze_to_silver.py"
aws s3 cp src/glue/silver_to_gold.py \
    "s3://${SCRIPTS_BUCKET}/glue/silver_to_gold.py"
aws s3 cp src/step_functions/pipeline_definition.json \
    "s3://${SCRIPTS_BUCKET}/step_functions/pipeline_definition.json"

# ── Package & upload Lambdas ──
info "Packaging Lambda functions …"
mkdir -p dist
(cd src/lambda/batch_ingestion && zip -q ../../dist/batch_ingestion.zip handler.py)
(cd src/lambda/pipeline_trigger && zip -q ../../dist/pipeline_trigger.zip handler.py)
aws s3 cp dist/batch_ingestion.zip  "s3://${SCRIPTS_BUCKET}/lambda/batch_ingestion.zip"
aws s3 cp dist/pipeline_trigger.zip "s3://${SCRIPTS_BUCKET}/lambda/pipeline_trigger.zip"
success "Artifacts uploaded"

# ── Deploy CloudFormation stack ──
info "Deploying CloudFormation stack: $STACK_NAME …"
aws cloudformation deploy \
    --template-file infrastructure/cloudformation/main-stack.yaml \
    --stack-name "$STACK_NAME" \
    --region "$AWS_REGION" \
    --capabilities CAPABILITY_NAMED_IAM \
    --parameter-overrides \
        ProjectName="$PROJECT_NAME" \
        Environment="${ENVIRONMENT:-dev}" \
        GlueScriptsBucket="$SCRIPTS_BUCKET" \
        AlertEmail="$ALERT_EMAIL" \
    --no-fail-on-empty-changeset

success "Stack deployed"

# ── Update Lambda code from S3 ──
info "Updating Lambda function code …"
aws lambda update-function-code \
    --function-name "${PROJECT_NAME}-batch-ingestion" \
    --s3-bucket "$SCRIPTS_BUCKET" \
    --s3-key lambda/batch_ingestion.zip \
    --region "$AWS_REGION" \
    --output table

aws lambda update-function-code \
    --function-name "${PROJECT_NAME}-pipeline-trigger" \
    --s3-bucket "$SCRIPTS_BUCKET" \
    --s3-key lambda/pipeline_trigger.zip \
    --region "$AWS_REGION" \
    --output table

success "Lambda functions updated"

# ── Print outputs ──
info "Stack outputs:"
aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --region "$AWS_REGION" \
    --query "Stacks[0].Outputs[*].[OutputKey,OutputValue]" \
    --output table

echo ""
echo -e "${BOLD}✅ Deployment complete!${NC}"
echo ""
echo "  Next steps:"
echo "   1. source .env"
echo "   2. python src/data_generator/generate_data.py --mode batch --rows 5000"
echo "   3. ./scripts/run_pipeline.sh"
echo "   4. Open Athena console → workgroup: ${PROJECT_NAME}-workgroup"
echo "      Run: SELECT * FROM ecommerce_gold.daily_revenue_summary LIMIT 30;"
echo ""
