package com.stanton.kafka.api;

import javax.validation.constraints.NotEmpty;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.dropwizard.Configuration;

public class KakfaConsumerConfiguration extends Configuration {
	@NotEmpty
	private String topic;
	@NotEmpty
	private String consumerGroup;
	@NotEmpty	
	private String bootstrapBrokers;
	
	@JsonProperty
	public String getTopic() {
		return topic;
	}
	@JsonProperty
	public void setTopic(String topic) {
		this.topic = topic;
	}
	@JsonProperty
	public String getConsumerGroup() {
		return consumerGroup;
	}
	@JsonProperty
	public void setConsumerGroup(String consumerGroup) {
		this.consumerGroup = consumerGroup;
	}
	@JsonProperty
	public String getBootstrapBrokers() {
		return bootstrapBrokers;
	}
	@JsonProperty
	public void setBootstrapBrokers(String bootstrapBrokers) {
		this.bootstrapBrokers = bootstrapBrokers;
	}
	
}
