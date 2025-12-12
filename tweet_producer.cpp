#include <iostream>
#include <string>
#include <librdkafka/rdkafkacpp.h>
using namespace std;


int main() {
    string brokers = "localhost:9092";
    string topic_name = "tweets";

    string error_string;
    RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL); //creating the configuration

    //telling our configuration where our kafka brokers are running
    if (conf->set("bootstrap.servers", brokers, error_string) != RdKafka::Conf::CONF_OK) { // the only setting we did here was where the kafka servers are running but there are so many other settings you can set
        std::cerr << "Failed to set brokers: " << error_string << std::endl;
        return 1;
    }

    //creating our producer
    RdKafka::Producer* producer = RdKafka::Producer::create(conf, error_string);
    if (!producer) {
        std::cerr << "Failed to create producer: " << error_string << std::endl;
        return 1;
    }

    //Send a message through the producer
    string message = "Hello from C++! This is my first Kafka message.";
    
    RdKafka::ErrorCode err = producer->produce(
        topic_name,                          // Topic name
        RdKafka::Topic::PARTITION_UA,        // Partition - topics have partitions for message sending parallelism (UA = UnAssigned let Kafka decide) - if the key is nullptr it will go roundrobin
        RdKafka::Producer::RK_MSG_COPY,      // Copy the payload
        const_cast<char*>(message.c_str()),  // Message payload
        message.size(),                      // Message size
        nullptr,                             // Key - this tells the specific partition of the topic the message goes to
        0,                                   // Key size
        0,                                   // Timestamp (0 = now)
        nullptr                              // Headers - key value metadata like http headers - this could be like source, version, etc.
    );

    producer->flush(5000);  //produce is async so flush to wait for the message to be sent
    
    //checking if anything is still in the queue
    if (producer->outq_len() > 0) {
        std::cerr << producer->outq_len() << " message(s) were not delivered" << std::endl;
    } else {
        std::cout << "Message delivered successfully!" << std::endl;
    }
    
    //Cleanup
    delete conf;
    delete producer;
    
    return 0;


}