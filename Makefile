CLUSTER_NAME=sparkify-cluster
EC2_INSTANCE_PROFILE=EMR_EC2_DefaultRole
KEY_NAME=sparkify
AWS_PROFILE=sparkify.emr

aws-set-profile:
	$(shell export AWS_PROFILE=$(AWS_PROFILE))
	echo $($$AWS_PROFILE)
	# j-23FSC1ZP0J76G

jupyter-notebook:
	poetry run jupyter notebook

emr-cluster-up:
	aws --profile $(AWS_PROFILE) emr create-cluster --name $(CLUSTER_NAME) \
	--use-default-roles \
	--release-label emr-5.28.0 \
	--instance-count 2 \
	--applications Name=Spark \
	--ec2-attributes KeyName=$(KEY_NAME) \
	--instance-type m3.xlarge \
	--instance-count 3 #\
	# --auto-terminate

emr-cluster-down:
	aws --profile $(AWS_PROFILE) emr terminate-clusters --cluster-ids $(cluster-id)

emr-cluster-describe:
	aws --profile $(AWS_PROFILE) emr describe-cluster --cluster-id $(cluster-id)

airflow:
	docker-compose -f docker-compose-airflow.yml up --build --force-recreate

airflow-down:
	docker-compose -f docker-compose-airflow.yml down -v

airflow-postgres-it:
	docker exec -it udacity-capstone_postgres_1 psql -d airflow -Uairflow

redshift-pause:
	aws redshift pause-cluster --cluster-identifier sparkify-dw --profile sparkify-dw-local --region us-west-2

redshift-resume:
	aws redshift resume-cluster --cluster-identifier sparkify-dw --profile sparkify-dw-local --region us-west-2

redshift-describe:
	aws redshift describe-clusters --cluster-identifier sparkify-dw --profile sparkify-dw-local --region us-west-2