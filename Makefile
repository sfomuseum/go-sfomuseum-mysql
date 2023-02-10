cli:
	go build -mod vendor -ldflags="-s -w" -o bin/sfom-mysql-index cmd/sfom-mysql-index/main.go
