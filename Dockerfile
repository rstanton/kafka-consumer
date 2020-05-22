FROM openjdk:15-alpine
RUN java -version
RUN apk add git
RUN git clone https://github.com/rstanton/kafka-consumer.git
WORKDIR /kafka-consumer

ENTRYPOINT ./gradlew run --args stock