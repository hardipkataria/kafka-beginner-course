package com.demos.kafka.opensearch;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.rmi.server.ExportException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {


    public static RestHighLevelClient createOpenSearchClient() {

        //local docker
        //String connString = "http://localhost:9200";

        //Bonsai OpenSearch URI -> credentials -> URL
        String connString = "https://rcvt7tfo5d:suyml2rc1z@kafka-course-5843847952.ap-southeast-2.bonsaisearch.net:443";

        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme())).setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp).setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


        }

        return restHighLevelClient;

    }

    //Create kafka Consumer
    private static KafkaConsumer<String, String> createKafkaConsumer(){
        //Mentioning GLobal variable
        String bootStrapServer="127.0.0.1:9092";
        String topic="second_topic";

        Properties consumerProperties = new Properties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"consumer-opensearch-demo");
        consumerProperties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");

        return new KafkaConsumer<String, String>(consumerProperties);
    }

    public static void main(String[] args) throws IOException {

        Logger log= LoggerFactory.getLogger(OpenSearchConsumer.class);

        //1. create OpenSearch client
        RestHighLevelClient openSearchClient = createOpenSearchClient();

        //2. create kafka client
        KafkaConsumer<String, String> kafkaConsumer=createKafkaConsumer();

        //we need to create index on openSearch if it does not exist
        boolean indexExist=openSearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);

        try{
            if(!indexExist){
                CreateIndexRequest createIndexRequest=new CreateIndexRequest("wikimedia");
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("wikimedia index is created");
            }else{
                log.info("wikimedia index already exist");
            }

            //subscribing to the topic where producer is pushing events
            kafkaConsumer.subscribe(Collections.singleton("wikimedia.recentchange"));

            while(true){
                ConsumerRecords<String, String> consumerRecords=kafkaConsumer.poll(Duration.ofMillis(3000));

                int recorCounts=consumerRecords.count();
                log.info("received: "+recorCounts +" records");

                //Create bulk request
                BulkRequest bulkRequest=new BulkRequest();


                for(ConsumerRecord<String, String> record: consumerRecords){
                    //send the record into OpenSearch

                    //Make consumer Idempotent
                    //Strategy 1
                    //define an ID using Kafka record coordinates

                    //String id=record.topic()+"_"+record.partition()+"_"+record.offset();

                    //Stretegy 2 -using id that is present in the record (extract from json)
                    String id=extractId(record.value());

                    IndexRequest indexRequest=new IndexRequest("wikimedia")
                            .source(record.value(), XContentType.JSON)
                            .id(id);

                    //IndexResponse indexResponse=openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
                    bulkRequest.add(indexRequest);
                   // log.info(indexResponse.getId());
                }

                if(bulkRequest.numberOfActions()>0) {
                    BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info("inserted "+bulkResponse.getItems().length +" records");

                    try{
                        Thread.sleep(1000);
                    }catch (Exception e){
                        e.printStackTrace();
                    }

                    //commit offset after batch is consumed
                    kafkaConsumer.commitAsync();
                    log.info("Offset are committed");
                }



            }


            //3. main login
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            //4. close things
            openSearchClient.close();
            kafkaConsumer.close();
        }



    }

    private static String extractId(String value) {

        return JsonParser.parseString(value)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();

    }


}
