# Makefile — Cloud Cost Platform

# ---------- Local Development ----------
install:
	pip install -r requirements.txt

setup: install
	python dynamo_manager.py

seed: setup
	python data_generator.py
	python anomaly_detector.py
	python recommendation_engine.py

run:
	python api.py

dashboard:
	streamlit run dashboard.py

# ---------- Full Pipeline ----------
all: seed run

# ---------- Testing ----------
test:
	pytest test_api.py -v

# ---------- Docker ----------
docker-up:
	docker-compose up -d

docker-down:
	docker-compose down

docker-build:
	docker build -t cloudwatch-app .

docker-logs:
	docker-compose logs -f

# ---------- Data Inspection ----------
show:
	python show_data.py

# ---------- Clean ----------
clean:
	docker-compose down -v
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true