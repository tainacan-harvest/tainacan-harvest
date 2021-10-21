.PHONY: setup
setup:
	docker-compose up -d --force-recreate --remove-orphans
	docker exec webserver sh -c "pip install pymongo"
	docker exec scheduler sh -c "pip install pymongo"

.PHONY: down
down:
	docker-compose down
