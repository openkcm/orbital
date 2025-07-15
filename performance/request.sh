curl -O -J --header "Content-Type: application/json" \
  --request POST \
  --data '@request.json' \
  http://localhost:8080/orbital/run
