#!/usr/bin/env bash
# ==============================================================================
# Last Server Standing — Native Progressive Saturation Benchmark (Bash)
#
# Progressively ramps load:
# - Stage 1:  1,000 requests
# - Stage 2:  5,000 requests
# - Stage 3+: Multiples of 4,000 (8,000 -> 12,000 -> 16,000 -> 20,000 -> 24,000...)
#
# Stops automatically when the server stops handling requests or error rate > 2%.
# ==============================================================================

set -uo pipefail

# ANSI Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
BOLD='\033[1m'
NC='\033[0m' # No Color

echo -e "\n${BOLD}${CYAN}======================================================================${NC}"
echo -e "${BOLD}${CYAN}      LAST SERVER STANDING — NATIVE PROGRESSIVE STRESS TEST           ${NC}"
echo -e "${BOLD}${CYAN}======================================================================${NC}\n"

# Prompt for host and port if not set
read -r -p "Enter Server Host [default: 127.0.0.1]: " INPUT_HOST
HOST="${INPUT_HOST:-127.0.0.1}"

read -r -p "Enter Server Port [default: 8080]: " INPUT_PORT
PORT="${INPUT_PORT:-8080}"

read -r -p "Enter Concurrency (parallel workers) [default: 50]: " INPUT_CONCURRENCY
CONCURRENCY="${INPUT_CONCURRENCY:-50}"

BASE_URL="http://${HOST}:${PORT}"

echo -e "\n${CYAN}==> Checking server health at ${BASE_URL}/health...${NC}"
HEALTH=$(curl -s --connect-timeout 3 "${BASE_URL}/health" 2>/dev/null || true)

if [[ "${HEALTH}" != *"\"status\":\"ok\""* && "${HEALTH}" != *"\"status\": \"ok\""* ]]; then
  echo -e "${RED}✗ Error: Server at ${BASE_URL}/health is not responding!${NC}"
  echo "Make sure your server is running ('cargo run --release')."
  exit 1
fi
echo -e "${GREEN}✓ Server is HEALTHY (200 OK)${NC}\n"

# Seed test products
echo -e "${CYAN}==> Seeding catalog dataset for stress testing...${NC}"
SEED_TIME=$(date +%s)
FLASH_PID="stress_flash_${SEED_TIME}"
REG_PID="stress_reg_${SEED_TIME}"

# 1. Flash product (50,000 stock)
curl -s -X POST "${BASE_URL}/products" \
  -H "Content-Type: application/json" \
  -d "{\"id\":\"${FLASH_PID}\",\"name\":\"Flash Item\",\"category\":\"smartphones\",\"price\":99999,\"stock\":50000}" > /dev/null

# 2. Regular product (100,000 stock)
curl -s -X POST "${BASE_URL}/products" \
  -H "Content-Type: application/json" \
  -d "{\"id\":\"${REG_PID}\",\"name\":\"Catalog Item\",\"category\":\"accessories\",\"price\":1500,\"stock\":100000}" > /dev/null

echo -e "${GREEN}✓ Created test products: ${FLASH_PID} and ${REG_PID}${NC}\n"

# Function to run a single worker batch
run_worker_batch() {
  local count="$1"
  local base_url="$2"
  local flash_pid="$3"
  local reg_pid="$4"
  local worker_id="$5"
  local out_file="$6"

  local ok_count=0
  local err_count=0
  local five_xx=0

  for ((i=1; i<=count; i++)); do
    local roll=$((RANDOM % 100))
    local code=0

    if [ "$roll" -lt 60 ]; then
      # 60% GET /products/:id
      code=$(curl -s -o /dev/null -w "%{http_code}" "${base_url}/products/${flash_pid}" 2>/dev/null || echo "000")
    elif [ "$roll" -lt 75 ]; then
      # 15% GET /products?category=...
      code=$(curl -s -o /dev/null -w "%{http_code}" "${base_url}/products?category=smartphones&page=1&limit=20" 2>/dev/null || echo "000")
    elif [ "$roll" -lt 95 ]; then
      # 20% POST /orders
      code=$(curl -s -o /dev/null -w "%{http_code}" -X POST "${base_url}/orders" \
        -H "Content-Type: application/json" \
        -d "{\"userId\":\"u_w${worker_id}_$((i % 50))\",\"productId\":\"${flash_pid}\"}" 2>/dev/null || echo "000")
    else
      # 5% GET /users/:id/orders
      code=$(curl -s -o /dev/null -w "%{http_code}" "${base_url}/users/u_w${worker_id}_$((i % 50))/orders" 2>/dev/null || echo "000")
    fi

    if [[ "$code" =~ ^(200|201|400|404|409)$ ]]; then
      ((ok_count++))
    else
      ((err_count++))
      if [[ "$code" =~ ^5 ]]; then
        ((five_xx++))
      fi
    fi
  done

  echo "${ok_count} ${err_count} ${five_xx}" > "${out_file}"
}

# Stages: 1000, 5000, 8000, 12000, 16000, 20000, 24000, 28000, 32000...
STAGES=(1000 5000)
for ((s=8000; s<=80000; s+=4000)); do
  STAGES+=("$s")
done

echo -e "${BOLD}Starting Progressive Load Stages (Concurrency: ${CONCURRENCY} workers):${NC}"
printf "%-10s | %-12s | %-16s | %-12s | %-10s | %s\n" "Stage" "Requests" "Throughput" "Duration" "Error %" "Status"
echo "----------------------------------------------------------------------------------------"

TMP_DIR=$(mktemp -d /tmp/stress_test_XXXXXX)
trap 'rm -rf "${TMP_DIR}"' EXIT

PEAK_RPS="0"
HIGHEST_SUCCESS=0
STAGE_NUM=1

for REQS in "${STAGES[@]}"; do
  PER_WORKER=$((REQS / CONCURRENCY))
  REMAINDER=$((REQS % CONCURRENCY))

  START_TIME=$(date +%s%N 2>/dev/null || python3 -c 'import time; print(int(time.time()*1e9))')

  PIDS=()
  for ((w=0; w<CONCURRENCY; w++)); do
    COUNT=$PER_WORKER
    if [ "$w" -lt "$REMAINDER" ]; then
      ((COUNT++))
    fi
    OUT_FILE="${TMP_DIR}/w_${w}.out"
    run_worker_batch "$COUNT" "${BASE_URL}" "${FLASH_PID}" "${REG_PID}" "$w" "${OUT_FILE}" &
    PIDS+=("$!")
  done

  # Wait for all workers in this stage
  for pid in "${PIDS[@]}"; do
    wait "$pid" 2>/dev/null || true
  done

  END_TIME=$(date +%s%N 2>/dev/null || python3 -c 'import time; print(int(time.time()*1e9))')
  ELAPSED_NS=$((END_TIME - START_TIME))
  
  # Calculate elapsed seconds with awk
  ELAPSED_S=$(awk -v ns="$ELAPSED_NS" 'BEGIN { printf "%.2f", ns / 1000000000 }')
  if (( $(awk -v s="$ELAPSED_S" 'BEGIN { print (s <= 0) }') )); then
    ELAPSED_S="0.01"
  fi

  RPS=$(awk -v reqs="$REQS" -v s="$ELAPSED_S" 'BEGIN { printf "%.1f", reqs / s }')

  # Aggregate counts
  TOTAL_OK=0
  TOTAL_ERR=0
  TOTAL_5XX=0
  for f in "${TMP_DIR}"/w_*.out; do
    if [ -f "$f" ]; then
      read -r ok err five < "$f"
      TOTAL_OK=$((TOTAL_OK + ok))
      TOTAL_ERR=$((TOTAL_ERR + err))
      TOTAL_5XX=$((TOTAL_5XX + five))
    fi
  done

  ERR_PCT=$(awk -v err="$TOTAL_ERR" -v tot="$REQS" 'BEGIN { printf "%.2f", (err * 100) / tot }')

  # Check failure: error rate > 2% or 5xx errors or zero successful requests
  FAILED=0
  if (( $(awk -v ep="$ERR_PCT" 'BEGIN { print (ep > 2.0) }') )) || [ "$TOTAL_OK" -eq 0 ] || [ "$TOTAL_5XX" -gt 0 ]; then
    FAILED=1
  fi

  if [ "$FAILED" -eq 1 ]; then
    STATUS="${RED}FAILED${NC}"
  else
    STATUS="${GREEN}PASSED${NC}"
    HIGHEST_SUCCESS="$REQS"
    if (( $(awk -v r="$RPS" -v p="$PEAK_RPS" 'BEGIN { print (r > p) }') )); then
      PEAK_RPS="$RPS"
    fi
  fi

  printf "#%-9d | %'-12d | %'11.1f req/s | %9.2f s | %8.2f%% | %b\n" \
    "$STAGE_NUM" "$REQS" "$RPS" "$ELAPSED_S" "$ERR_PCT" "$STATUS"

  if [ "$FAILED" -eq 1 ]; then
    echo -e "\n${BOLD}${RED}==> SERVER SATURATION REACHED AT ${REQS} REQUESTS!${NC}"
    echo -e "    Error Rate: ${ERR_PCT}% (5xx: ${TOTAL_5XX}, Failed/Dropped: ${TOTAL_ERR})"
    echo -e "    Highest Sustained Successful Load: ${BOLD}${HIGHEST_SUCCESS} requests${NC}"
    echo -e "    Peak Throughput Observed:          ${BOLD}${GREEN}${PEAK_RPS} req/sec${NC}\n"
    break
  fi

  ((STAGE_NUM++))
  sleep 0.5
done

if [ "$FAILED" -eq 0 ]; then
  echo -e "\n${BOLD}${GREEN}==> ALL STAGES PASSED! Server handled up to ${HIGHEST_SUCCESS} requests seamlessly!${NC}"
  echo -e "    Peak Throughput Observed: ${BOLD}${GREEN}${PEAK_RPS} req/sec${NC}\n"
fi
