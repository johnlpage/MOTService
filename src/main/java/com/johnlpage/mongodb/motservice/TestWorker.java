package com.johnlpage.mongodb.motservice;

import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.LogManager;

import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Logger;

public class TestWorker implements Runnable {
    MOTDataAccessInterface motdal;
    int itterations = 0;
    long[] vehicleids;
    Logger logger;
    int threadNo;
    CommandLineOptions options;
    long newTestId = 2000000000L;

    public TestWorker(MOTDataAccessInterface fetcher, long[] vehicleids, CommandLineOptions options, int threadNo) {
        this.motdal = fetcher;
        this.threadNo = threadNo;
        this.options = options;
        this.vehicleids = vehicleids;
        newTestId += threadNo;
        this.logger = (Logger) LoggerFactory.getLogger(TestWorker.class);
    }

    public void run() {
        // If these add up to 100 it's easier to think about
        // Insert and Update make individual calls to the server
        // So this would not be the way to test/code for bulk insert speed for example


       
        // Timing from inside one testworker

        long startTime = System.currentTimeMillis(); // Cheap but less aggurate than nanotime good enough
        long endTime = startTime + options.getTestLength() * 1000;

        while (System.currentTimeMillis() < endTime) {
            
           
            int idIndex = ThreadLocalRandom.current().nextInt(0, vehicleids.length);
            
            if(this.threadNo < options.getReadRatio())
            {

                this.motdal.getMOTResultInJSON("" + vehicleids[idIndex]);
            }
            else if(this.threadNo < options.getReadRatio() + options.getCreateRatio()) 
            {
                
                this.motdal.createNewMOTResult(newTestId,vehicleids[idIndex]);
                newTestId += options.getnThreads();
            } else {
                this.motdal.updateMOTResult(vehicleids[idIndex]);
            }

            itterations++;
        }

        if (threadNo == 0) {
            logger.info(String.format("Test length %d seconds, %d total READ requests. Throughput %d requests per second",
                    options.getTestLength(),
                    itterations * options.getReadRatio(),
                    itterations * options.getReadRatio()/ options.getTestLength()));
          
        } else if(threadNo == options.getReadRatio()) {
            logger.info(String.format("Test length %d seconds, %d total CREATE requests. Throughput %d requests per second",
            options.getTestLength(),
            itterations * options.getCreateRatio(),
            itterations *options.getCreateRatio() / options.getTestLength()));
        } else if(threadNo == options.getReadRatio() + options.getCreateRatio()) {
            logger.info(String.format("%d,%d,Test length %d seconds, %d total UPDATE requests. Throughput %d requests per second",
            itterations,options.getUpdateRatio(),
            options.getTestLength(),
            itterations * options.getUpdateRatio(),
            itterations * options.getUpdateRatio() / options.getTestLength()));
        } 
    }
}
