.PHONY: all
all: run-ingest


.PHONY: build
build:
	@echo "Building"
	@go build -o build/konnect main.go

.PHONY: run
run-ingest: build
	@./build/konnect -mode ingest

.PHONY: run-index
run-index: build
	@./build/konnect -mode index

.PHONY: clean
clean:
	rm -f build/konnect
