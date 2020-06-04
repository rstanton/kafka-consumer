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
import org.apache.kafka.common.serialization.Serdes.LongSerde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.internals.MaterializedInternal;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import com.codahale.metrics.annotation.Timed;
import com.google.gson.Gson;

/**
 * Event consumer that builds a materialised view of stock by store & EAN
 * @author ross
 *
 */
@Path("/stores")
@Produces(MediaType.APPLICATION_JSON)
public class StoreStockResource {
	private Gson json = new Gson();
	private KafkaStreams streams;
	private KGroupedStream<String, String> gStream;
	private KTable stockTable; 
	
	public String viewName;
	
	private KakfaConsumerConfiguration config;
	private static final Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
	
	public StoreStockResource(KakfaConsumerConfiguration config) {
		this.config = config;

		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "store-stream-processor");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapBrokers());
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		
		StreamsBuilder builder = new StreamsBuilder();
		//build(builder);
		buildStockMaterializedView(builder);
		
		streams = new KafkaStreams(builder.build(), props);
		streams.start();

	}
	
	@GET
	@Timed
	@Path("/stock")
	public List<Store> getStores(){
				
		if(viewName!=null) {
			ReadOnlyKeyValueStore<String, Double> store =  streams.store(StoreQueryParameters.fromNameAndType(viewName, QueryableStoreTypes.keyValueStore()));
			
			KeyValueIterator<String, Double> values = store.all();
			while(values.hasNext()) {
				KeyValue<String, Double> kv = values.next();
				logger.info(kv.key +": "+kv.value);
			}
		}
	
		return Arrays.asList(new Store());
	}
	
	private void getStockForStoreEAN(String store, String ean) {
		logger.info("Store Stock Table materialized as "+stockTable.queryableStoreName());
		
		if(stockTable.queryableStoreName()!=null) {
			ReadOnlyKeyValueStore<String, Double> view = streams.store(StoreQueryParameters.fromNameAndType(stockTable.queryableStoreName(), QueryableStoreTypes.<String, Double>keyValueStore()));
			KeyValueIterator<String, Double> it = view.all();
			while(it.hasNext()) {
				KeyValue<String, Double> kv = it.next();
				logger.info("Store - EAN "+kv.key+" has "+kv.value+" stock");
			}
		}
	}
	
	private void buildStockMaterializedView(StreamsBuilder builder) {
		try {
			//Group the stream based on the store and EAN
			KGroupedStream<String, String> stockStream = builder.<String,String>stream(config.getTopic()).groupBy(new KeyValueMapper<String, String, String>(){
				@Override
				public String apply(String key, String value) {
					Stock stock = json.fromJson(value, Stock.class);
					return stock.getStoreCode()+"-"+stock.getEAN();
				}
			});
			
			stockTable = stockStream.aggregate(
			new Initializer<Double>() {
				@Override
				public Double apply() {
					return 0.0;
				}
				
			}, new Aggregator<String, String, Double>(){
				@Override
				public Double apply(String key, String value, Double aggregate) {
					Stock stock = json.fromJson(value,  Stock.class);
					return aggregate+stock.getQty();
				}
				
			},Materialized.<String, Double, KeyValueStore<Bytes, byte[]>>as("store-stock-table").withValueSerde(Serdes.Double()));
			
			viewName = stockTable.queryableStoreName();
		}
		catch(Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}
	
	private void build(StreamsBuilder builder) {
		try {
			// Group by Store Code
			logger.info("Starting to build views");
			
			gStream = builder.<String, String>stream(config.getTopic()).groupBy(new KeyValueMapper<String, String, String>() {
				@Override
				public String apply(String key, String value) {
					Stock stock = json.fromJson(value, Stock.class);
					//tlogger.info("Processing Stream, new Key "+stock.getStoreCode());
					return stock.getStoreCode();
				}

			});
			
			/**KStream<String, LongviewName> storeStream = gStream.count().toStream();
			storeStream.foreach(new ForeachAction<String, Long>(){

				@Override
				public void apply(String key, Long value) {
					logger.info("Count for "+key +": "+value);
				}
				
			});**/
			
			Materialized<String,Long, KeyValueStore<Bytes, byte[]>> storeCount = Materialized.as("store-stock-record-count");
			
			KTable<String, Long> recordCount = gStream.count(storeCount);
			
			logger.info("Queryable Store Name: "+recordCount.queryableStoreName());	
			viewName = recordCount.queryableStoreName();
			
		}
		catch(Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}
}
