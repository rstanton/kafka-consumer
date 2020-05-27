package com.stanton.kafka.api;

import io.dropwizard.Application;
import io.dropwizard.setup.Environment;

public class KafkaConsumerApplication extends Application<KakfaConsumerConfiguration> {

	@Override
	public void run(KakfaConsumerConfiguration configuration, Environment environment) throws Exception {	
		final KafkaStockConsumerResource stock = new KafkaStockConsumerResource(configuration);
		environment.jersey().register(stock);
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
