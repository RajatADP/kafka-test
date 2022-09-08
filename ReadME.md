# Basic skeleton with kafka-java

 `Framework used - testng`

 `testng.xml is executed which will run all tests mentioned in file`



Starting tests from Dockerfile

1. Bring up zookeeper and kafka server on local
2. build image from Dockerfile
    `docker build -t kafka-test:latest .`
3. run container from image
    `docker run --name kafka-test-container kafka-test:latest`
4. producer and consumer kafka tests should execute and in container logs will be there.


Starting tests from Docker Compose
1. Bring up zookeeper and kafka server on local
2. `docker-compose up` will trigger tests and image creation

