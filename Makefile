run: build 
	@./bin/GoRedis --listenAddr :5001 
	
build:
	@go build -o bin/GoRedis .
 
