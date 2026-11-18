#!/usr/bin/env bash
set -e

API_URL="${API_URL:-http://localhost:8080}"
echo "Running Last Server Standing verification against: $API_URL"

# 1. Health check
echo "==> Checking GET /health"
HEALTH=$(curl -s "$API_URL/health")
echo "Health response: $HEALTH"
if [[ "$HEALTH" != *"\"status\":\"ok\""* && "$HEALTH" != *"\"status\": \"ok\""* ]]; then
  echo "FAIL: Health check failed"
  exit 1
fi
echo "PASS: Health check ok"

# 2. Test 1: The Last Unit Test
echo "==> Running Test 1: The Last Unit (50 concurrent buyers, 1 unit)"
curl -s -X POST "$API_URL/products" -H 'content-type: application/json' \
  -d '{"id":"plast_test_1","name":"Last One","category":"ctest","price":100,"stock":1}' > /dev/null

args=()
for i in $(seq 50); do
  [ "$i" -gt 1 ] && args+=(--next)
  args+=(-s -o /dev/null -w '%{http_code}\n' -X POST -H 'content-type: application/json' \
         -d "{\"userId\":\"u_last_$i\",\"productId\":\"plast_test_1\"}" "$API_URL/orders")
done

RESULTS=$(curl -Z --parallel-immediate --parallel-max 50 "${args[@]}" | sort | uniq -c)
echo "Test 1 Results:"
echo "$RESULTS"

# 3. Test 2: Double-Click Test (20 concurrent requests, same user & product)
echo "==> Running Test 2: The Double Click (20 identical concurrent requests)"
curl -s -X POST "$API_URL/products" -H 'content-type: application/json' \
  -d '{"id":"pclick_test_1","name":"Click Test","category":"ctest","price":100,"stock":1000}' > /dev/null

args=()
for i in $(seq 20); do
  [ "$i" -gt 1 ] && args+=(--next)
  args+=(-s -o /dev/null -w '%{http_code}\n' -X POST -H 'content-type: application/json' \
         -d '{"userId":"udouble_test_1","productId":"pclick_test_1"}' "$API_URL/orders")
done

CLICK_RESULTS=$(curl -Z --parallel-immediate --parallel-max 20 "${args[@]}" | sort | uniq -c)
echo "Test 2 Results:"
echo "$CLICK_RESULTS"

STOCK_CHECK=$(curl -s "$API_URL/products/pclick_test_1")
echo "Product stock after click test: $STOCK_CHECK"

ORDERS_CHECK=$(curl -s "$API_URL/users/udouble_test_1/orders")
echo "User orders after click test: $ORDERS_CHECK"

echo "==> Verification complete!"
