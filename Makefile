.PHONY: all test test-integration  clean docker-compose-up clean-docker-compose

all: test

# env variables for local deployment
db_host = localhost
db_port = 5432
db_user = postgres
db_pass = secret
db_name = orbital
config = DB_HOST=$(db_host) DB_PORT=$(db_port) DB_USER=$(db_user) DB_PASS=$(db_pass) DB_NAME=$(db_name)

# The tests use go-testcontainers, which require Docker to be running. 
# Additional setup may be needed depending on your system, refer to https://golang.testcontainers.org/system_requirements/ for details.

# run tests
test:
	go test -p 1 -count=1 -race -shuffle=on -coverprofile=cover.out ./...

# run tests with verbose output
test-verbose:
	go test -v -p 1 -count=1 -race -shuffle=on -coverprofile=cover.out ./...	

# run specific test function defined by TEST_FUNC variable
test-single:
	go test -p 1 -count=1 -race -shuffle=on -coverprofile=cover.out -run $(TEST_FUNC) ./...

# run integration tests
test-integration:
	go test -tags=integration ./integration/... -v

clean:
	rm -rf $(CERTS_DIR) cover.out cover.html

docker-compose-up:
	$(config) docker compose up -d

# clean up all containers and volumes
clean-docker-compose:
	($(config) docker compose down -v && $(config) docker compose rm -f -v)
