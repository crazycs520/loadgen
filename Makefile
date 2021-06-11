
default: build

all: default

build:
	go build -o bin/load main.go