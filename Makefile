jupyter-notebook:
	poetry run jupyter notebook

airflow:
	docker-compose -f docker-compose-airflow.yml up --build --force-recreate

airflow-down:
	docker-compose -f docker-compose-airflow.yml down -v