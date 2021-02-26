SHELL := /bin/bash

default: run

all:

run:
#	docker build -f ./consumer_producer_service/Dockerfile -t "consumer_producer_service:latest" .
#	docker build -f ./business_download_service/Dockerfile -t "business_download_service:latest" .
#	docker build -f ./business_joiner_service/Dockerfile -t "business_joiner_service:latest" .
	docker build -f ./monitor_service/Dockerfile -t "monitor_service:latest" .
	docker-compose -f docker-compose.yml up -d --build
.PHONY: run

logs:
	docker-compose -f docker-compose.yml logs -f
.PHONY: logs

stop:
	docker-compose -f docker-compose.yml stop -t 1
	docker-compose -f docker-compose.yml down
.PHONY: stop
