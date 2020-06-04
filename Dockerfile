FROM openjdk:15-alpine
RUN apk add --no-cache libstdc++
RUN apk add --no-cache libc6-compat
RUN apk add --no-cache gcompat
RUN java -version
RUN apk add git
RUN git clone https://github.com/rstanton/kafka-consumer.git
WORKDIR /kafka-consumer
EXPOSE 9000/tcp
ENTRYPOINT ./gradlew run --args="server ./StockConsumer.yml"
