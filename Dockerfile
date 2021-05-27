FROM public.ecr.aws/lambda/java:8

COPY target/scala-2.12/classes ${LAMBDA_TASK_ROOT}
COPY target/scala-2.12/athena-delta-lake-assembly-0.1-deps.jar ${LAMBDA_TASK_ROOT}/lib/

CMD [ "com.backmarket.athena.connector.delta.DeltaMetadataHandler::handleRequest" ]