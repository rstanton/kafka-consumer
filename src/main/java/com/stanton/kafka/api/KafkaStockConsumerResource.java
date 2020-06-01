package com.stanton.kafka.api;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import java.util.logging.Logger;

import com.codahale.metrics.annotation.Timed;
import com.google.gson.Gson;


@Path("/stock")
@Produces(MediaType.APPLICATION_JSON)
public class KafkaStockConsumerResource {
	private Consumer<String, String> consumer;
	private static final Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
	
	public KafkaStockConsumerResource(KakfaConsumerConfiguration config) {
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", config.getBootstrapBrokers());
		props.setProperty("group.id", config.getConsumerGroup());
		props.setProperty("request.timeout.ms", "30000");
		props.setProperty("enable.auto.commit", "true");
		props.setProperty("auto.commit.interval.ms", "1000");
		props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		consumer = new KafkaConsumer<>(props);
		Map<String, List<PartitionInfo>> topics = consumer.listTopics();
		 
		consumer.subscribe(Arrays.asList(config.getTopic()));
	}
	
	@GET
	@Timed
	@Path("/events")
	public List<Stock> getStockEventsForStores() {
		return  Arrays.asList(pollKafka());
	}
	
	/**
	 * Reads the raw stream of stock events
	 * @return
	 */
	private Stock[] pollKafka() {
    	try {
    		 ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
    		 
    		 Stock[] results = new Stock[records.count()];
    		 
    		 logger.info("Got "+records.count()+" records on poll for stock");
    		 
    		 int index =0;
    		 
    		 for(ConsumerRecord<String, String> record: records) {
    			 Gson json = new Gson();
    			 results[index++] = json.fromJson(record.value(),Stock.class);
    		 }
    		 
    		 return results;
	   	}
	   	catch(Exception e) {
	   		e.printStackTrace();
	   		return null;
	   	}
	}
}
