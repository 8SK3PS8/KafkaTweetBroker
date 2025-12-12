#include <iostream>
#include <string>
#include <csignal>
#include <librdkafka/rdkafkacpp.h>
#include <hiredis/hiredis.h>

using namespace std;

// Global flag to stop the consumer gracefully when you hit Ctrl+C
bool running = true;

void signal_handler(int) {
    cout << "\nStopping consumer..." << endl;
    running = false;
}

class RedisCache{
    private:
        redisContext* ctx;
    
    public:
        //constructor
        RedisCache(const string& host, int port)
        {
            ctx = redisConnect(host.c_str(), port);
            if (ctx == nullptr) 
            {
                cerr << "Can't allocate redis context" << endl;
            } 
            else if (ctx->err) 
            {
                cerr << "Redis connection error: " << ctx->errstr << endl;
                redisFree(ctx);
                ctx = nullptr;
            }
        }

        ~RedisCache() 
        {
            if (ctx) 
            {
                redisFree(ctx);
            }
        }

        bool isConnected() 
        {
            return ctx != nullptr;
        }

        void storeTweet(const string& tweet)
        {
            if(!ctx) 
            {
                return;
            }
        
            redisReply* reply = (redisReply*)redisCommand(ctx, "LPUSH tweets %s", tweet.c_str()); // push the %s value to the front of the list
            if (reply) 
            {
                freeReplyObject(reply); //have to free the allocated mem from each reply
            }
            
            reply = (redisReply*)redisCommand(ctx, "LTRIM tweets 0 99"); // keep only items 0-99 on the list so trim
            if (reply) 
            {
                freeReplyObject(reply); //have to free the allocated mem from each reply
            }
        }

        vector<string> getRecentTweets(int count)
        {
            vector<string> tweets;
            if(!ctx)
            {
                return tweets;
            }

            redisReply* reply = (redisReply*)redisCommand(ctx, "LRANGE tweets 0 %d", count - 1);
            if(reply && reply->type == REDIS_REPLY_ARRAY)
            {
                for(size_t i = 0; i < reply->elements; i++)
                {
                    tweets.push_back(reply->element[i]->str);
                }
            }

            if(reply)
            {
                freeReplyObject(reply); // have to free the allocated memory from every reply
            }

            return tweets;
        }



};

int main()
{
    string brokers = "localhost:9092";
    string topic_name = "tweets";
    string group_id = "my-consumer-group";

    signal(SIGINT, signal_handler); // this handles the Ctrl + C


    //Connect to Redis
    RedisCache cache("localhost", 6379);
    if(!cache.isConnected())
    {
        cerr << "Failed to connect to Redis. Is it running?" << endl;
        cerr << "Start it with: docker-compose up -d" << endl;
        return 1;
    }

    string error_string;
    RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL); // This creates the configuration

    //Now we need to do all of the settings for the Kafka Configurations
    if(conf->set("bootstrap.servers", brokers, error_string) != RdKafka::Conf::CONF_OK)
    {
        cerr << "Failed to set brokers: " << error_string << endl;
        return 1;
    }

    //so basically you have a group and consumers can be part of the group, each group gets all the messages
    //its basically like an organizational thing so that you can do things seperately
    if (conf->set("group.id", group_id, error_string) != RdKafka::Conf::CONF_OK) {
        cerr << "Failed to set group.id: " << error_string << endl;
        return 1;
    }

    //This setting configures where to start reading if this consumer group has never consumed before
    // "earliest" = start from the beginning of the topic
    // "latest" = only read new messages from now on
    if (conf->set("auto.offset.reset", "earliest", error_string) != RdKafka::Conf::CONF_OK) {
        cerr << "Failed to set auto.offset.reset: " << error_string << endl;
        return 1;
    }

    // Create the consumer
    RdKafka::KafkaConsumer* consumer = RdKafka::KafkaConsumer::create(conf, error_string);
    if (!consumer) {
        cerr << "Failed to create consumer: " << error_string << endl;
        return 1;
    }

    cout << "Consumer created: " << consumer->name() << endl;

    // Subscribe to the topic (can subscribe to multiple topics)
    vector<string> topics = {topic_name};
    RdKafka::ErrorCode err = consumer->subscribe(topics);
    if (err != RdKafka::ERR_NO_ERROR) {
        cerr << "Failed to subscribe: " << RdKafka::err2str(err) << endl;
        return 1;
    }

    while(running)
    {
        RdKafka::Message* msg = consumer->consume(1000); // poll for a message, if nothings there wait for 1 second

        switch (msg->err()) 
        {
            case RdKafka::ERR_NO_ERROR: {
                string payload(static_cast<const char*>(msg->payload()), msg->len());

                cache.storeTweet(payload);
                cout << "Received: " << payload << endl;

                vector<string> recent = cache.getRecentTweets(5);
                cout << "Recent tweets in cache:" << endl;
                for (size_t i = 0; i < recent.size(); i++) 
                {
                    cout << "  " << i + 1 << ". " << recent[i] << endl;
                }
                break;
            }

            case RdKafka::ERR__TIMED_OUT:
                //No message within timeout - just keep waiting
                break;

            case RdKafka::ERR__PARTITION_EOF:
                //Reached end of partition
                cout << "Reached end of partition" << endl;
                break;

            default:
                cerr << "Consumer error: " << msg->errstr() << endl;
                break;
        }

        delete msg;

        }

    cout << "Closing consumer..." << endl;
    consumer->close();
    delete consumer;
    delete conf;

    cout << "Done!" << endl;
    return 0;
}