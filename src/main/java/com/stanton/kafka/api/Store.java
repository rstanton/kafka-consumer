package com.stanton.kafka.api;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Store {

	private String storeCode;

	@JsonProperty
	public String getStoreCode() {
		return storeCode;
	}

	@JsonProperty
	public void setStoreCode(String storeCode) {
		this.storeCode = storeCode;
	}
	
	
}
