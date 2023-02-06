package com.johnlpage.mongodb.motservice;

import static com.mongodb.client.model.Projections.*;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;

import com.mongodb.client.DistinctIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoDBFetcher implements MOTFetcherInterface {

    private MongoClient mongoClient = null;
    private Logger logger;
    private final String databaseName = "mot";
    private final String collectionName = "testresult";


    /*
     * We use RawBSONDocument Here not Document (or a speciic Class) as
     * we want to convert it directly into JSON and that's all - so rather than
     * Instantiate all the Objects to convert it from the database BSON bytes
     * into Java Classes Map/String/Integer etc only to then create a JSON string
     * We can go from BSON bytestream directly to JSON which is more efficient
     * RawBSONDocument is immutable but if we wanted to filter or reshape we could
     * also do that by projection, this can cut the appserver CPU use by 80% or more
     */

    private MongoCollection<RawBsonDocument> testresults;

    MongoDBFetcher(String URI) {
        logger = LoggerFactory.getLogger(MongoDBFetcher.class);

        try {
            mongoClient = MongoClients.create(URI);
            // Creatinng a client doesn't actually connect until it needs to so we ping the
            // server
            // This will also fail with incorrect auth - although not with No Auth
            logger.info(mongoClient.getDatabase("admin").runCommand(new Document("ping", 1)).toJson());
            testresults = mongoClient.getDatabase(databaseName).getCollection(collectionName, RawBsonDocument.class);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage());
            mongoClient = null;
        }
    }

    public boolean initialised() {
        return mongoClient != null;
    }

    @Override
    public String getMOTResultInJSON(String identifier) {


        long identifierLong;
        try {
            identifierLong = Long.valueOf(identifier);

            Bson byIdQuery = Filters.eq("vehicleid", identifierLong);

            RawBsonDocument result = testresults.find(byIdQuery).limit(1).first();
            if (result != null) {
                return result.toJson();
            }
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage());
        }
        return "{ }"; // Not found
    }

    @Override
    public long[] getVehicleIdentifiers() {
         /*
         * This isn't pretty just because I want a massive array of long values in as
         * little RAM as possible It's a Java ugliness rather than a Database one
         * 
         * The value range is potentially too large to use a BitSet - largestid is ~1.4
         * Billion in the data I have - that's quite sparse (40M in 1.4Bn ) also index
         * for a BitSet is integer not long so Max size is 2.1B bits - would work 
         * but not worth the risk
         * 
         * Heap used for ArrayList<Long> was 2190MB taking 92 seconds
         * Heap used for long[] is 1663MB taking 66 seconds
         * 
         */
    

        logger.info("Fetching list of Vehicle IDs from database");
        MongoCollection<Document> tr = mongoClient.getDatabase(databaseName).getCollection(collectionName);
        int nDocs = (int) tr.estimatedDocumentCount(); //EDC is fine on data that doesn't have ops going on
        int idx = 0;

        Bson filter = Filters.empty();
        Bson projection = fields(include("vehicleid"), exclude("_id"));
        MongoCursor<Document> resultiter = tr.find(filter).projection(projection).iterator();

        long[] vehicleids = new long[nDocs];
        for(idx=0;idx<vehicleids.length;idx++)
        {
            Long l = resultiter.next().getLong("vehicleid"); // Might be null
            if (l == null) {
                l = 0L;
            }
            vehicleids[idx] = l;
        }
        
        return vehicleids;  
    }
}
