package com.stanton.kafka.api;

import java.io.ObjectInputFilter.Config;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import com.codahale.metrics.annotation.Timed;
import com.google.gson.Gson;

/**
 * API that reads the KAFKA stock stream to determine what stores exist by grouping and then aggregating the stream
 * @author ross
 *
 */
@Path("/stores")
@Produces(MediaType.APPLICATION_JSON)
public class StoreResource {
	private Gson json = new Gson();
	private KafkaStreams streams;
	private KGroupedStream<String, String> gStream;
	
	public String viewName;
	
	private KakfaConsumerConfiguration config;
	private static final Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
	
	public StoreResource(KakfaConsumerConfiguration config) {
		this.config = config;
		
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "store-stream-processor");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapBrokers());
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		
		StreamsBuilder builder = new StreamsBuilder();
		build(builder);
		
		streams = new KafkaStreams(builder.build(), props);
		streams.start();
	}
	
	@GET
	@Timed
	public List<Store> getStores(){
		
		if(viewName!=null) {
			ReadOnlyKeyValueStore<String, Long> store =  streams.store(StoreQueryParameters.fromNameAndType(viewName, QueryableStoreTypes.keyValueStore()));
			
			KeyValueIterator<String, Long> values = store.all();
			while(values.hasNext()) {
				KeyValue<String, Long> kv = values.next();
				logger.info(kv.key);
			}
		}
		else {
			KTable<String, Long> storeTable = gStream.count();
			
			logger.info("Got Table name "+storeTable.queryableStoreName());
			if(storeTable.queryableStoreName()!=null) {
				logger.info("Table name: "+storeTable.queryableStoreName());
				
				viewName = storeTable.queryableStoreName();
			}
		}
		
		return Arrays.asList(new Store());
	}
	
	private void build(StreamsBuilder builder) {
		try {
			// Group by Store Code
			logger.info("Starting to build views");
			
			gStream = builder.<String, String>stream(config.getTopic()).groupBy(new KeyValueMapper<String, String, String>() {
				@Override
				public String apply(String key, String value) {
					Stock stock = json.fromJson(value, Stock.class);
					logger.info("Processing Stream, new Key "+stock.getStoreCode());
					return stock.getStoreCode();
				}

			});
			
			KTable<String, Long> storeTable = gStream.count();
			
			logger.info("Got Table name "+storeTable.queryableStoreName());
			if(storeTable.queryableStoreName()!=null) {
				logger.info("Table name: "+storeTable.queryableStoreName());
				
				viewName = storeTable.queryableStoreName();
			}
			
		}
		catch(Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}
}
