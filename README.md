This was a simple C++ mini-project to learn more about Kafka and Caching. Recently, I made a [presentation](https://docs.google.com/presentation/d/1dC_b9wlbYxxbbX1X7Ff1d89jNer-OTv4JUzC83Mm1rM/edit?usp=sharing)) about how Netflix uses Write-Ahead Logging to build a resilient data platform (), and in this project I wanted to apply some of the logic from that presentation. More specficially, I created a project that sends data through a kafka broker and updates the cache (Redis). More about this can be found on my recent [blog post](https://adeobam.com/blog/kafka-redis-cpp).

In addition to the core concepts of Kafka and Caching, there is also some practical Docker knowledge that I learned through creating a configuration file to spin up three containers that configured my Kafka broker, Redis Cache, and Kafka UI.

## Flow of Data

```
Producer  ->  Kafka  -> Consumer  ->  Redis
```

Producer sends tweets to Kafka. Consumer reads them and stores in Redis.

## To Setup

Install the dependencies:
```bash
brew install librdkafka hiredis
```

## To Run

Start Kafka and Redis:
```bash
docker-compose up -d
```

Compile:
```bash
g++ -std=c++17 tweet_producer.cpp -o tweet_producer $(pkg-config --cflags --libs rdkafka++)
g++ -std=c++17 tweet_consumer.cpp -o tweet_consumer $(pkg-config --cflags --libs rdkafka++ hiredis)
```

Run the consumer in one terminal:
```bash
./tweet_consumer
```

Run the producer in another terminal:
```bash
./tweet_producer
```

Kafka UI should be visible at: http://localhost:8080

## Stop

```bash
docker-compose down
```