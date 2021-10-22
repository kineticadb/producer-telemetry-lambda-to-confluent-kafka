export KAFKA_SASL_PASS=XXX
export KAFKA_SASL_USER=XXX
export KAFKA_HOST=pkc-187.us-east-2.aws.confluent.cloud
export KAFKA_PORT=9092
export KAFKA_TOPIC=telemetry-mobile

python lambda_function.py
