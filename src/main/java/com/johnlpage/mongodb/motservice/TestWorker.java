package com.johnlpage.mongodb.motservice;

import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.LogManager;

import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Logger;

public class TestWorker implements Runnable {
    MOTDataAccessInterface motdal;
    int itterations;
    long[] vehicleids;
    Logger logger;
    int threadNo;
    int nThreads;
    long newTestId = 2000000000L;

    public TestWorker(MOTDataAccessInterface fetcher, long[] vehicleids, int itterations,int nThreads,int threadNo) {
        this.motdal = fetcher;
        this.threadNo = threadNo;
        this.nThreads=nThreads;
        this.itterations = itterations;
        this.vehicleids = vehicleids;
        newTestId += threadNo;
        LogManager.getLogManager().reset();
        this.logger = (Logger) LoggerFactory.getLogger(TestWorker.class);

    }

    public void run() {
        // If these add up to 100 it's easier to think about
        // Insert and Update make individual calls to the server
        //So this would not be the way to test/code for bulk insert speed for example 

        final int READ_RATIO = 80;
        final int INSERT_RATIO = 15;
        final int UPDATE_RATIO = 5;

        

        String json = new String("ERROR FETCHING JSON");
        // Timing from inside one testworker

        long startTime = System.nanoTime();
        for (int i = 0; i < itterations; i++) {
            int operation = ThreadLocalRandom.current().nextInt(0, READ_RATIO + UPDATE_RATIO + INSERT_RATIO);

            if (operation < READ_RATIO) {
                int idIndex = ThreadLocalRandom.current().nextInt(0, vehicleids.length);
               
                json = this.motdal.getMOTResultInJSON("" + vehicleids[idIndex]);
              
            } else if (operation < READ_RATIO + UPDATE_RATIO) {
                this.motdal.createNewMOTResult(newTestId);
                newTestId += nThreads;
            } else {
                
                this.motdal.updateMOTResult();
            }
        }
        long endTime = System.nanoTime();
        double secs = (double) (endTime - startTime) / 1000000000.0;
        if(threadNo == 0) {
            logger.info(String.format("Mixed workload completed in %.3f seconds , %d requests, Throughput %d requests per second",secs,itterations*nThreads,(int)((double)(nThreads*itterations)/secs)));
            this.logger.debug(json); // This is there to ensure it's actually working
        }
    }
}
