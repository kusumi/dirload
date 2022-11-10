bin:
	go build
fmt:
	go fmt
test:
	go test -v
clean:
	go clean

lint:
	golangci-lint run
xxx:	fmt lint test
