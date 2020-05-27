package com.stanton.kafka.api;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Stock {
	private String EAN;
	private String location;
	private double qty;
	private String storeCode;
	
	public Stock() {
		
	}

	@JsonProperty
	public String getEAN() {
		return EAN;
	}
	
	@JsonProperty
	public void setEAN(String ean) {
		EAN = ean;
	}
	@JsonProperty
	public String getLocation() {
		return location;
	}
	@JsonProperty
	public void setLocation(String location) {
		this.location = location;
	}
	@JsonProperty
	public double getQty() {
		return qty;
	}
	@JsonProperty
	public void setQty(double qty) {
		this.qty = qty;
	}
	@JsonProperty
	public String getStoreCode() {
		return storeCode;
	}
	@JsonProperty
	public void setStoreCode(String storeCode) {
		this.storeCode = storeCode;
	}
	
	
}
