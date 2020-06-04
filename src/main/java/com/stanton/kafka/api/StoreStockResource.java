package com.stanton.kafka.api;

import java.io.ObjectInputFilter.Config;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
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
	private KTable<String, Double> stockTable; 
	private ReadOnlyKeyValueStore<String, Double> view;
	
	public String viewName;

	private KakfaConsumerConfiguration config;
	private static Logger logger;
	
	public StoreStockResource(KakfaConsumerConfiguration config) {
		logger = Logger.getLogger(this.getClass().getName());
		
		this.config = config;

		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "store-stream-processor");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapBrokers());
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		
		StreamsBuilder builder = new StreamsBuilder();
		//build(builder);
		stockTable = buildStockMaterializedView(builder);
		
		streams = new KafkaStreams(builder.build(), props);
		streams.start();
		
		try {
			while(streams.state()!=KafkaStreams.State.RUNNING) {
				Thread.sleep(100);
			}

			view = streams.store(StoreQueryParameters.fromNameAndType(stockTable.queryableStoreName(), QueryableStoreTypes.<String, Double>keyValueStore()));
		}
		catch(Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}

	}
	
	/**
	 * Actually needs to return something useful
	 * @return
	 */
	@GET
	@Timed
	@Path("/stock")
	public List<Store> getStores(){

		if(view!=null) {
			KeyValueIterator<String, Double> values = view.all();
			while(values.hasNext()) {
				KeyValue<String, Double> kv = values.next();
				logger.info(kv.key +": "+kv.value);
			}
	
			return Arrays.asList(new Store());
		}
		return null;
	}
	
	/** 
	 * Returns the stock quantity for a given Store & EAN
	 * 
	 * @param store
	 * @param ean
	 */
	@GET
	@Timed
	@Path("/{storeid}/{ean}")
	public Double getStockForStoreEAN(@PathParam("storeid") String store, @PathParam("ean")String ean) {
		logger.info("Getting stock for "+store+" and "+ean);
		return view.get(store+"-"+ean);
	}
	
	/**
	 * 
	 * @param builder
	 * @return
	 */
	private KTable<String, Double> buildStockMaterializedView(StreamsBuilder builder) {
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
			
			return stockTable;
		}
		catch(Exception e) {
			e.printStackTrace();
			return null;
		}
	}
}

