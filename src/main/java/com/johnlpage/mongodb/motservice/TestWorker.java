package com.johnlpage.mongodb.motservice;


import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.LogManager;

import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Logger;

public class TestWorker implements Runnable  {
    MOTFetcherInterface fetcher;
    int itterations;
    long[] vehicleids;
    Logger logger;

    public TestWorker(MOTFetcherInterface fetcher,long[] vehicleids, int itterations) {
        this.fetcher = fetcher;
        this.itterations = itterations;
        this.vehicleids = vehicleids;
        LogManager.getLogManager().reset();
		this.logger = (Logger) LoggerFactory.getLogger(TestWorker.class);
       
    }



    public void run() {
        String json = new String("ERROR FETCHING JSON");
        for(int i=0;i<itterations;i++) {
            int idIndex =ThreadLocalRandom.current().nextInt(0, vehicleids.length);
            json = this.fetcher.getMOTResultInJSON(""+vehicleids[idIndex]);
        }
        this.logger.info(json); //This is there to ensure it's actually working
    }
}
