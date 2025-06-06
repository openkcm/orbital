# Directory to store generated certificates
CERTS_DIR := certs
# Validity duration for certs (days)
DAYS      := 365

.PHONY: all test test-integration generate-certs clean docker-compose-up clean-docker-compose

all: test

# env variables for local deployment
db_host = localhost
db_port = 5432
db_user = postgres
db_pass = secret
db_name = orbital
config = DB_HOST=$(db_host) DB_PORT=$(db_port) DB_USER=$(db_user) DB_PASS=$(db_pass) DB_NAME=$(db_name)

test: docker-compose-up
	$(config) go test -v -p 1 -count=1 -race -shuffle=on -coverprofile=cover.out ./...

# Run integration tests (requires brokers up)
test-integration: docker-compose-up
	@echo "Waiting for Solace AMQP (plain) on localhost:5673…"
	@bash -c 'for i in $$(seq 1 30); do nc -z 127.0.0.1 5673 && break || echo -n . && sleep 1; done'
	@echo "Waiting for RabbitMQ AMQP (plain) on localhost:5672…"
	@bash -c 'for i in $$(seq 1 30); do nc -z 127.0.0.1 5672 && break || echo -n . && sleep 1; done'
	@echo "\n Brokers are up! Running integration tests…"
	go test -tags=integration ./integration/... -v

# Generate CA, server, and client certificates for mTLS
generate-certs:
	mkdir -p $(CERTS_DIR)
	# 1) root CA
	openssl genrsa -out $(CERTS_DIR)/rootCA.key 2048
	openssl req -x509 -new -nodes -key $(CERTS_DIR)/rootCA.key \
	    -subj "/CN=Test Root CA" -days 3650 -out $(CERTS_DIR)/rootCA.pem

	# 2) server cert (for RabbitMQ) with SAN
	openssl genrsa -out $(CERTS_DIR)/server.key 2048
	openssl req -new -key $(CERTS_DIR)/server.key \
	    -subj "/CN=localhost" -out $(CERTS_DIR)/server.csr
	# Create SAN config for server cert
	printf "[v3_req]\nsubjectAltName=DNS:localhost,IP:127.0.0.1\n" > $(CERTS_DIR)/server_ext.cnf
	openssl x509 -req \
	    -in $(CERTS_DIR)/server.csr \
	    -CA $(CERTS_DIR)/rootCA.pem \
	    -CAkey $(CERTS_DIR)/rootCA.key \
	    -CAcreateserial \
	    -days $(DAYS) \
	    -extfile $(CERTS_DIR)/server_ext.cnf \
	    -extensions v3_req \
	    -out $(CERTS_DIR)/server.pem

	# 3) client cert (for your client library)
	openssl genrsa -out $(CERTS_DIR)/client.key 2048
	openssl req -new -key $(CERTS_DIR)/client.key \
	    -subj "/CN=guest" -out $(CERTS_DIR)/client.csr
	openssl x509 -req -in $(CERTS_DIR)/client.csr \
	    -CA $(CERTS_DIR)/rootCA.pem \
	    -CAkey $(CERTS_DIR)/rootCA.key \
	    -CAcreateserial \
	    -days $(DAYS) \
	    -out $(CERTS_DIR)/client.pem

clean:
	rm -rf $(CERTS_DIR) cover.out cover.html

# initialize and start infra container
docker-compose-up: generate-certs
	$(config) docker compose up -d

# clean up all containers and volumes
clean-docker-compose:
	($(config) docker compose down -v && $(config) docker compose rm -f -v)
