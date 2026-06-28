#!/usr/bin/env bash
# ============================================================
# run_pipeline.sh — Trigger a pipeline run and tail the status.
# Usage:
#   ./scripts/run_pipeline.sh                    # yesterday
#   ./scripts/run_pipeline.sh 2024-11-01         # specific date
#   ./scripts/run_pipeline.sh 2024-10-01 2024-10-31  # backfill range
# ============================================================
set -euo pipefail

BLUE='\033[0;34m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
RED='\033[0;31m'; BOLD='\033[1m'; NC='\033[0m'
info()    { echo -e "${BLUE}[INFO]${NC} $*"; }
success() { echo -e "${GREEN}[OK]${NC}  $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC} $*"; }
error()   { echo -e "${RED}[ERR]${NC}  $*"; exit 1; }

[[ -f .env ]] && source .env

PROJECT_NAME="${PROJECT_NAME:-ecommerce-pipeline}"
AWS_REGION="${AWS_REGION:-us-east-1}"
AWS_ACCOUNT="${AWS_ACCOUNT:-$(aws sts get-caller-identity --query Account --output text)}"

# Get SFN ARN from stack output
SFN_ARN=$(aws cloudformation describe-stacks \
    --stack-name "${PROJECT_NAME}-stack" \
    --region "$AWS_REGION" \
    --query "Stacks[0].Outputs[?OutputKey=='StateMachineArn'].OutputValue" \
    --output text)

[[ -z "$SFN_ARN" ]] && error "Could not retrieve StateMachineArn. Is the stack deployed?"

# Build trigger payload
if [[ $# -eq 0 ]]; then
    YESTERDAY=$(date -d "yesterday" +%Y-%m-%d 2>/dev/null || date -v-1d +%Y-%m-%d)
    PAYLOAD="{\"processing_date\": \"${YESTERDAY}\"}"
    info "Running pipeline for yesterday: $YESTERDAY"
elif [[ $# -eq 1 ]]; then
    PAYLOAD="{\"processing_date\": \"${1}\"}"
    info "Running pipeline for date: $1"
elif [[ $# -eq 2 ]]; then
    PAYLOAD="{\"start_date\": \"${1}\", \"end_date\": \"${2}\"}"
    info "Backfill from $1 to $2"
else
    error "Usage: $0 [date] [end_date]"
fi

# Start execution
EXEC_ARN=$(aws stepfunctions start-execution \
    --state-machine-arn "$SFN_ARN" \
    --name "manual-$(date +%Y%m%d-%H%M%S)" \
    --input "$PAYLOAD" \
    --region "$AWS_REGION" \
    --query "executionArn" \
    --output text)

success "Execution started: $EXEC_ARN"
echo ""

# Poll until done
echo -e "${BOLD}Polling execution status (Ctrl+C to stop watching) …${NC}"
echo ""
PREV_STATUS=""
while true; do
    STATUS=$(aws stepfunctions describe-execution \
        --execution-arn "$EXEC_ARN" \
        --region "$AWS_REGION" \
        --query "status" \
        --output text)

    if [[ "$STATUS" != "$PREV_STATUS" ]]; then
        TIMESTAMP=$(date +"%H:%M:%S")
        case "$STATUS" in
            RUNNING)   echo -e "  ${BLUE}[$TIMESTAMP] ⚙  RUNNING${NC}" ;;
            SUCCEEDED) echo -e "  ${GREEN}[$TIMESTAMP] ✅ SUCCEEDED${NC}"; break ;;
            FAILED)    echo -e "  ${RED}[$TIMESTAMP] ❌ FAILED${NC}"
                       aws stepfunctions describe-execution \
                           --execution-arn "$EXEC_ARN" \
                           --region "$AWS_REGION" \
                           --query "cause" \
                           --output text
                       exit 1 ;;
            ABORTED)   warn "Execution ABORTED"; break ;;
            TIMED_OUT) error "Execution TIMED_OUT" ;;
        esac
        PREV_STATUS="$STATUS"
    fi
    sleep 15
done

echo ""
echo -e "${BOLD}Done! Query your data in Athena:${NC}"
echo "  aws athena start-query-execution \\"
echo "    --query-string 'SELECT * FROM ecommerce_gold.daily_revenue_summary LIMIT 10' \\"
echo "    --work-group ${PROJECT_NAME}-workgroup \\"
echo "    --region $AWS_REGION"
echo ""
