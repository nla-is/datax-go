.PHONY: deploy

deploy:
	mkdir -p $(CURDIR)/build
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o $(CURDIR)/build/repeater.amd64
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o $(CURDIR)/build/repeater.arm64
	dataxctl deploy au --stateful repeater $(CURDIR)
