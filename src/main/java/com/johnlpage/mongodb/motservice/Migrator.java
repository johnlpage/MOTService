package com.johnlpage.mongodb.motservice;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;

/* This is used to mograte from RDBMS to MongoDB
 * There aother options but this way once you write the RDBMS->JSON
 * code you can basically reuse that
 */

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;

import com.mongodb.client.model.Indexes;

import java.sql.ResultSetMetaData;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class Migrator {

    private Logger logger;
    boolean initialised = false;
    boolean isMySQL = false;
    private Connection connection = null;
    PreparedStatement getTestStmt = null;
    private MongoClient mongoClient = null;
    private MongoCollection<Document> destinationCollection = null;

    private final String databaseName = "mot";
    private final String collectionName = "testresult";

    private final String getAllRecords =  "select " +
    "tr.*, " +
    "ft.FUEL_TYPE, " +
    "tt.TESTTYPE AS TYPENAME, " +
    "to2.RESULT, " +
    "ti.*, " +
    "fl.*, " +
    "tid.MINORITEM,tid.RFRDESC,tid.RFRLOCMARKER,tid.RFRINSPMANDESC,tid.RFRADVISORYTEXT,tid.TSTITMSETSECID, " +
    "b.ITEMNAME AS LEVEL1, " +
    "c.ITEMNAME AS LEVEL2, " +
    "d.ITEMNAME AS LEVEL3, " +
    "e.ITEMNAME AS LEVEL4, " +
    "f.ITEMNAME AS LEVEL5  " +
    "from TESTRESULT tr " +
    "LEFT JOIN TESTITEM ti  on ti.TESTID = tr.TESTID " +
    "LEFT JOIN FUEL_TYPES ft on ft.TYPECODE = tr.FUELTYPE " +
    "LEFT JOIN TEST_TYPES tt on tt.TYPECODE  = tr.TESTTYPE " +
    "LEFT JOIN TEST_OUTCOME to2 on to2.RESULTCODE  = tr.TESTRESULT " +
    "LEFT JOIN FAILURE_LOCATION fl on ti.LOCATIONID = fl.FAILURELOCATIONID " +
    "LEFT JOIN TESTITEM_DETAIL AS tid ON ti.RFRID = tid.RFRID AND tid.TESTCLASSID = tr.TESTCLASSID " +
    "LEFT JOIN TESTITEM_GROUP AS b ON tid.TSTITMID = b.TSTITMID AND tid.TESTCLASSID = b.TESTCLASSID " +
    "LEFT JOIN TESTITEM_GROUP AS c ON b.PARENTID = c.TSTITMID AND b.TESTCLASSID = c.TESTCLASSID " +
    "LEFT JOIN TESTITEM_GROUP AS d ON c.PARENTID = d.TSTITMID AND c.TESTCLASSID = d.TESTCLASSID " +
    "LEFT JOIN TESTITEM_GROUP AS e ON d.PARENTID = e.TSTITMID AND d.TESTCLASSID = e.TESTCLASSID " +
    "LEFT JOIN TESTITEM_GROUP AS f ON e.PARENTID = f.TSTITMID AND e.TESTCLASSID = f.TESTCLASSID " ;

    Migrator(String URI, String TargetURI) {
        logger = LoggerFactory.getLogger(Migrator.class);

        try {
            if (URI.contains("mysql://")) {
                isMySQL = true;
                logger.info("RDBMS is MySQL");
            }
            connection = DriverManager.getConnection(URI);

            getTestStmt = connection.prepareStatement(getAllRecords, java.sql.ResultSet.TYPE_FORWARD_ONLY,
                    java.sql.ResultSet.CONCUR_READ_ONLY);
            if (isMySQL) {
                getTestStmt.setFetchSize(Integer.MIN_VALUE);
            }

            mongoClient = MongoClients.create(TargetURI);
            logger.info(mongoClient.getDatabase("admin").runCommand(new Document("ping", 1)).toJson());
            this.destinationCollection = mongoClient.getDatabase(databaseName).getCollection(collectionName);

        } catch (SQLException e) {
            logger.error("Unable to connect to RDBMS");
            logger.error(e.getLocalizedMessage());
            connection = null;
        } catch (Exception e) {
            logger.error("Other Error");
            logger.error(e.getLocalizedMessage());
            connection = null;
        }
    }

    public boolean initialised() {

        return connection != null;
    }

    public boolean migrateData() {
        Long currentTestId = -1L;
        Document mongoDoc = null; // Document, like JSONObj is based on a Map
        List<Document> insertBatch = new ArrayList<Document>();
        destinationCollection.drop();
        // Check we aren't a new thread - if we are we need a new conneciton.
        try {

            ResultSet testResult = getTestStmt.executeQuery();
            ResultSetMetaData metaData = testResult.getMetaData();
            // Create JSON from a set of Rows
            String[] topFieldNames = { "TESTID", "VEHICLEID", "TESTTYPE", "TESTRESULT", "TESTDATE", "TESTCLASSID",
                    "TYPENAME",
                    "TESTMILEAGE", "POSTCODEREGION", "MAKE", "MODEL", "COLOUR", "FUELTYPE", "FUEL_TYPE", "CYLCPCTY",
                    "FIRSTUSEDATE",
                    "RESULT" };

            String[] itemFieldNames = { "RFRID", "RFRTYPE", "DMARK", "LOCATIONID", "LAT", "LONGITUDINAL", "VERTICAL",
                    "MINORITEM", "RFRDESC",
                    "RFRLOCMARKER",
                    "RFRINSPMANDESC", "RFRADVISORYTEXT", "LEVEL1", "LEVEL2", "LEVEL3", "LEVEL4", "LEVEL5" };

            boolean firstRow = true;
            List<Document> itemList = null;
            int count = 0;
            while (testResult.next()) {
                // Are we starting a new Test?
                Long testId = testResult.getLong("TESTID");
                if (!testId.equals(currentTestId)) {

                    if (mongoDoc != null) {
                        mongoDoc.append("testitems", itemList);
                        // Also as the PK is _id in MongoDB copy testid in there
                        mongoDoc.put("_id",mongoDoc.get("testid"));
                        insertBatch.add(mongoDoc);
                        if (insertBatch.size() == 1000) {
                            destinationCollection.insertMany(insertBatch);
                            insertBatch = new ArrayList<Document>();
                            count = count + 1000;
                            logger.info("Migrated " + count);
                        }
                    }
                    mongoDoc = new Document();
                    firstRow = true;
                    itemList = new ArrayList<Document>();

                    currentTestId = testId;
                }

                Document itemDoc = new Document(); // New Item per row

                for (int col = 1; col <= metaData.getColumnCount(); col++) {
                    String label = metaData.getColumnLabel(col);

                    if (firstRow && Arrays.asList(topFieldNames).contains(label)) {
                        Object val = testResult.getObject(col);
                        if (val != null) {
                            mongoDoc.put(label.toLowerCase(), val);
                        }
                    }
                    // All Rows add to the Items array - this is a simple JSON structure
                    // Wiith just one top level array of objects

                    if (Arrays.asList(itemFieldNames).contains(label)) {
                        Object val = testResult.getObject(col);
                        if (val != null) {
                            itemDoc.put(label.toLowerCase(), val);
                        }
                    }
                }

                /* If our item isnt blank add it to the items list */
                if (itemDoc.getInteger("rfrid", -1) != -1) {
                    itemList.add(itemDoc);

                }
                firstRow = false;
            }

            testResult.close();

            // Write any remaining records to MongoDB
            if (insertBatch.size() >0 ) {
                destinationCollection.insertMany(insertBatch);
                count = count + insertBatch.size();
                logger.info("Migrated " + count);
            }

            /* Add an index on vehicleid */
            logger.info("Creating index on vehicleid field");
            destinationCollection.createIndex(Indexes.ascending("vehicleid"));
            logger.info("Index complete");
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.toString());
        }
        return true;
    }
}
