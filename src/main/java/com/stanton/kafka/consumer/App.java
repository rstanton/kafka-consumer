/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package com.stanton.kafka.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.logging.Logger;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;

public class App {
	private static final Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
	private String id;
	
	public App(String consumerId) {
		this.id = consumerId;
	}
	
    public void run() {logger.info("Polling for records");
    	try {
  		
	    	 Properties props = new Properties();
	    	 props.setProperty("bootstrap.servers", "localhost:29092");
	    	 props.setProperty("group.id", id);
	    	 props.setProperty("request.timeout.ms", "30000");
	         props.setProperty("enable.auto.commit", "true");
	         props.setProperty("auto.commit.interval.ms", "1000");
	    	 props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	    	 props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	
	    	 Consumer<String, String> consumer = new KafkaConsumer<>(props);
	    	 Map<String, List<PartitionInfo>> topics = consumer.listTopics();
	    	 
	    	 for(Entry<String, List<PartitionInfo>> entry: topics.entrySet()) {
	    		 logger.info("Topics: "+entry.getKey());
	    	 }
	    	 
	    	 logger.info("Got "+topics.size()+" topics");
	    	 ConsumerGroupMetadata md = consumer.groupMetadata();
	    	 logger.info("Group ID "+md.groupId());

	    	 consumer.subscribe(Arrays.asList("stock"));

	    	 while(true) {
	    		 ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
	    		 
	    		 for(ConsumerRecord<String, String> record: records) {
	    			 logger.info("Offset "+record.offset()+". key "+record.key()+" , with value "+ record.value());
	    		 }
	    	 }
    	}
    	catch(Exception e) {
    		e.printStackTrace();
        }
    }

    private void listTopics() {
    	
    }
    public static void main(String[] args) {
    	logger.info("Starting Kafka Listener with Consumer ID "+args[0]);
        new App(args[0]).run();
    }
}
