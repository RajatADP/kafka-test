FROM maven:3.8.6-openjdk-11

WORKDIR /kafka-test

COPY . .

RUN mvn clean install

CMD ["mvn", "test"]
