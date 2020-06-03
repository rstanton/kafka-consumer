package com.stanton.kafka.api;

import io.dropwizard.Application;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

public class KafkaConsumerApplication extends Application<KakfaConsumerConfiguration> {
    @Override
    public void initialize(Bootstrap<KakfaConsumerConfiguration> bootstrap) {
        // Enable variable substitution with environment variables
        bootstrap.setConfigurationSourceProvider(
                new SubstitutingSourceProvider(bootstrap.getConfigurationSourceProvider(),
                                                   new EnvironmentVariableSubstitutor(false)
                )
        );

    }
    
	@Override
	public void run(KakfaConsumerConfiguration configuration, Environment environment) throws Exception {	
		final StockEventsResource stock = new StockEventsResource(configuration);
		environment.jersey().register(stock);
		
		final StoreStockResource stores = new StoreStockResource(configuration);
		environment.jersey().register(stores);
	}
	
	public static void main(String[] args) {
		try {
			new KafkaConsumerApplication().run(args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(-1);
		};
	}

}
