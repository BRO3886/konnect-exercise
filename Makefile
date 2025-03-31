.PHONY: all
all: run-ingest


.PHONY: build
build:
	@echo "Building"
	@go build -o build/konnect main.go

.PHONY: run
run-ingest: build
	@./build/konnect -mode ingest -workers 1

.PHONY: run-index
run-index: build
	@./build/konnect -mode index -workers 1

.PHONY: clean
clean:
	rm -f build/konnect
