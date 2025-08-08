curl -O -J --header "Content-Type: application/json" \
  --connect-timeout 10240 \
  --max-time 10240 \
  --request POST \
  --data '@request.json' \
  http://localhost:8080/orbital/run
