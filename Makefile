bin:
	go build
clean:
	go clean
fmt:
	go fmt
lint:
	golangci-lint run
test:
	go test -v

xxx:	fmt lint test
