package com.johnlpage.mongodb.motservice;

import java.util.List;

public interface MOTFetcherInterface {
    public String getMOTResultInJSON(String identifier);
    public long[] getVehicleIdentifiers();
    public boolean initialised();

}