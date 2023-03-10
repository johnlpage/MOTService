package com.johnlpage.mongodb.motservice;

import static com.mongodb.client.model.Projections.*;

import java.util.concurrent.TimeUnit;

import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;
import org.bson.types.MinKey;

import com.mongodb.ReadPreference;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import static com.mongodb.client.model.Sorts.ascending;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoDBDataAccessLayer implements MOTDataAccessInterface {

    private static MongoClient mongoClient = null; // Singleton
    private Logger logger;
    private final String databaseName = "mot";
    private final String collectionName = "testresult";
    private RawBsonDocument testObj;

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
    private MongoCollection<Document> testresultsw;

    MongoDBDataAccessLayer(String URI, boolean readFromSecondariesToo) {
        logger = LoggerFactory.getLogger(MongoDBDataAccessLayer.class);

        try {
            if (mongoClient == null) {
                mongoClient = MongoClients.create(URI);
            }
            // Creatinng a client doesn't actually connect until it needs to so we ping the
            // server
            // This will also fail with incorrect auth - although not with No Auth
             String pingme = mongoClient.getDatabase("admin").runCommand(new Document("ping", 1)).toJson();
            // Use this for reading - don't auto parse the repsonse into Objects
            testresults = mongoClient.getDatabase(databaseName).getCollection(collectionName, RawBsonDocument.class);
            if (readFromSecondariesToo) {
                // We can say read from any secondary that is no more than X millis behind in
                // replicaiton.
                // or 0 for anything
                logger.debug("Setting Read Preference to nearest");
                testresults = testresults.withReadPreference(ReadPreference.nearest(120, TimeUnit.SECONDS));
            }

            // Use this one for writing
            testresultsw = mongoClient.getDatabase(databaseName).getCollection(collectionName);

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

            testObj = testresults.find(byIdQuery).limit(1).first();
            if (testObj != null) {
                return testObj.toJson();
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
        int nDocs = (int) tr.estimatedDocumentCount(); // EDC is fine on data that doesn't have ops going on
        int idx = 0;

        Bson filter = Filters.empty();
       //MongoDB will only cover a query if it's already using the index
        Bson projection = fields(include("vehicleid"), exclude("_id"));
       
        MongoCursor<Document> resultiter = tr.find(filter).projection(projection).sort(ascending("vehicleid")).iterator();

        long[] vehicleids = new long[nDocs];
        for (idx = 0; idx < vehicleids.length; idx++) {
            Long l = resultiter.next().getLong("vehicleid"); // Might be null
            if (l == null) {
                l = 0L;
            }
            vehicleids[idx] = l;
        }

        return vehicleids;
    }

    @Override
    public boolean createNewMOTResult(Long testId,Long vehicleId) {
        if (testObj == null)
            {
                getMOTResultInJSON(""+vehicleId); //Grab a template example
            }
        try {
            Document newTest = new Document(testObj); // We chose to read immutable RAW documents
            newTest.put("testid", testId);
            newTest.put("_id", testId);
            newTest.put("vehicleid", vehicleId);

            InsertOneResult a = testresultsw.insertOne(newTest);
            if(a.wasAcknowledged() == false) { logger.warn("An Insert failed to happen");return false;}
        } catch (Exception e) {
            logger.error(e.getClass().toString());
            logger.error(e.getMessage());
            return false;
        }
        return true;
    }

    @Override
    public void resetTestDatabase() {
        try {
            logger.info("Deleting records created during test");
            // Using the PK index on _id rather than addin gone on testid
            Bson testDocsQuery = Filters.gte("_id", 2000000000L);
            testresults.deleteMany(testDocsQuery);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    // Updating by testid (_id) is more logical as it's a specific test you would be changing
    // But we don't have a list of testids, we do have a ist of vehicleid's  and performance
    // will be the same
    @Override
    public boolean updateMOTResult(Long vehicleId) {
        try {

            Bson query = Filters.eq("vehicleid", vehicleId);
            Bson update = Updates.inc("testmileage", 1);
            UpdateResult r =  testresults.updateOne(query, update);
            if(r.getModifiedCount() == 0) { logger.warn("An Update failed to take place");}
        } catch (Exception e) {
            logger.error(e.getMessage());
            return false;
        }
        return true;
    }
}
