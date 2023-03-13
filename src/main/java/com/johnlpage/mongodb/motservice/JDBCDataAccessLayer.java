package com.johnlpage.mongodb.motservice;

import java.util.Arrays;
import java.util.Date;
import java.util.ArrayList;

import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;

/* 
* This is JDBC and I've developed it with MySQL as that's what the
 * sameple data documentation sugget and MySQL 8 isn't bad at all
 * But I'm sure you could make it work with Postgres too
 */

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSetMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class JDBCDataAccessLayer implements MOTDataAccessInterface {

    private Logger logger;
    boolean initialised = false;
    boolean isMySQL = false;
    private Connection connection = null;
    private List<PreparedStatement> readConnections = null;

    PreparedStatement insertResultStmt = null;
    PreparedStatement insertItemStmt = null;
    PreparedStatement updateStmt = null;

    JSONObject jsonObj = null; /*
                                * Keep the last thing we read so we can base a write off it ,
                                * means we are being real and mosty editing things we have recently read too
                                */

    private final String getlatestByVehicleSQL = "select " +
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
            "LEFT JOIN TESTITEM_GROUP AS f ON e.PARENTID = f.TSTITMID AND e.TESTCLASSID = f.TESTCLASSID " +
            "WHERE  tr.TESTID = (SELECT TESTID FROM TESTRESULT WHERE VEHICLEID=? LIMIT 1)";

    private final String insertResultSQL = "INSERT INTO TESTRESULT (TESTID,VEHICLEID,TESTDATE,TESTCLASSID,TESTTYPE,TESTRESULT,TESTMILEAGE,POSTCODEREGION,MAKE,"
            +
            "MODEL,COLOUR,FUELTYPE,CYLCPCTY,FIRSTUSEDATE) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

    private final String insertItemSQL = "INSERT INTO TESTITEM (TESTID,RFRID,RFRTYPE,LOCATIONID,DMARK) VALUES (?,?,?,?,?)";

    private final String updateResultSQL = "UPDATE TESTRESULT SET TESTMILEAGE = TESTMILEAGE+1 WHERE VEHICLEID= ?";

    JDBCDataAccessLayer(String URI, String replicaURIs) {
        logger = LoggerFactory.getLogger(JDBCDataAccessLayer.class);

        try {
            if (URI.contains("mysql://")) {
                isMySQL = true;
                
            }
            Class.forName("org.postgresql.Driver"); //Required for some reason
            connection = DriverManager.getConnection(URI);
            connection.setAutoCommit(false);
            readConnections = new ArrayList<PreparedStatement>();
            PreparedStatement getTestStmt = connection.prepareStatement(getlatestByVehicleSQL,
                    java.sql.ResultSet.TYPE_FORWARD_ONLY,
                    java.sql.ResultSet.CONCUR_READ_ONLY);
            readConnections.add(getTestStmt);

            if (replicaURIs != null) {

                String[] connectionStrings = replicaURIs.split(",");
                for (String c : Arrays.asList(connectionStrings)) {
                    Connection newCon = DriverManager.getConnection(c);
                    PreparedStatement getTestStmtRep = newCon.prepareStatement(getlatestByVehicleSQL,
                            java.sql.ResultSet.TYPE_FORWARD_ONLY,
                            java.sql.ResultSet.CONCUR_READ_ONLY);
                    readConnections.add(getTestStmtRep);
                }
            }

            insertResultStmt = connection.prepareStatement(insertResultSQL, java.sql.ResultSet.TYPE_FORWARD_ONLY,
                    java.sql.ResultSet.CONCUR_READ_ONLY);

            insertItemStmt = connection.prepareStatement(insertItemSQL, java.sql.ResultSet.TYPE_FORWARD_ONLY,
                    java.sql.ResultSet.CONCUR_READ_ONLY);

            updateStmt = connection.prepareStatement(updateResultSQL, java.sql.ResultSet.TYPE_FORWARD_ONLY,
                    java.sql.ResultSet.CONCUR_READ_ONLY);

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

    /*
     * When we add tests they are all with testid so we can remove them again >
     * 2,000,000,000
     */
    public void resetTestDatabase() {
        try {
            logger.info("Removing previous test records");
            Statement removeCreated = connection.createStatement();
            removeCreated.execute("DELETE FROM TESTRESULT WHERE TESTID >= 2000000000");
            removeCreated.execute("DELETE FROM TESTITEM WHERE TESTID >= 2000000000");
            connection.commit();
            logger.info("Complete");
        } catch (Exception e) {
            logger.error(e.getMessage());
            System.exit(1);
        }
    }

    public boolean initialised() {

        return connection != null;
    }

    private void bindGenericParam(PreparedStatement st, int idx, Object val) throws Exception {
        switch (val.getClass().getSimpleName()) {
            case "Integer":
                st.setInt(idx, (Integer) val);
                break;
            case "Long":
                st.setLong(idx, (Long) val);
                break;
            case "String":
                st.setString(idx, (String) val);
                break;
            case "Date":
                st.setDate(idx, (java.sql.Date) (Date) val);
                break;
            default:
                throw new Exception("Data type " + val.getClass().getSimpleName() + " not implemented");
        }
    }

    public boolean createNewMOTResult(Long testId,Long vehicleId) {
        if (jsonObj == null){
            getMOTResultInJSON(""+vehicleId);  //We need one as a template to create new ones
        }

        try {

            JSONArray items = jsonObj.optJSONArray("testitems");

            String resultFields[] = { "TESTID", "VEHICLEID", "TESTDATE", "TESTCLASSID", "TESTTYPE", "TESTRESULT",
                    "TESTMILEAGE", "POSTCODEREGION", "MAKE", "MODEL", "COLOUR", "FUELTYPE", "CYLCPCTY",
                    "FIRSTUSEDATE" };
            String itemFields[] = { "TESTID", "RFRID", "RFRTYPE", "LOCATIONID", "DMARK" };
            
            for (int c = 0; c < resultFields.length; c++) {
                Object o = jsonObj.get(resultFields[c].toLowerCase());
                if (resultFields[c].toLowerCase().equals("testid")) {
                    o = testId;
                }
                else if (resultFields[c].toLowerCase().equals("vehicleid")) {
                    o = vehicleId;
                }
                bindGenericParam(insertResultStmt, c + 1, o);
            }
            insertResultStmt.execute();

            if (items != null) {
                for (Object item : items) {
                    JSONObject jo = (JSONObject) item;
                    for (int c = 0; c < itemFields.length; c++) {
                        Object o;
                        if (itemFields[c].toLowerCase().equals("testid")) {
                            o = testId;
                        } else {
                            o = jo.get(itemFields[c].toLowerCase());
                        }
                        bindGenericParam(insertItemStmt, c + 1, o);
                    }

                    insertItemStmt.addBatch();
                }
                insertItemStmt.executeBatch();
            }

            connection.commit();
        } catch (Exception ex) {
            // Cancel any transactions
            try {
                connection.rollback();
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
            }
            logger.error(ex.getMessage(), ex);
            logger.info(jsonObj.toString(2));
            return false;
        }
        /* Take the last one we read (or read one ) and add a new one based on it */
        return false;
    }

     // Updating by testid  is more logical as it's a specific test you would be changing
    // But we don't have a list of testids, we do have a ist of vehicleid's  and performance
    // will be the same
    public boolean updateMOTResult(Long vehicleId) {

        try {
            updateStmt.setLong(1, vehicleId);
            updateStmt.execute();
            connection.commit();
            return true;
        } catch (SQLException ex) {
            logger.error(ex.getMessage(), ex);
        }

        return false;

    }

    public String getMOTResultInJSON(String identifier) {
        long identifierLong;
        jsonObj = new JSONObject();
        // Check we aren't a new thread - if we are we need a new conneciton.
        try {
            identifierLong = Long.valueOf(identifier);
            //Pick a prepared statement from out list of readers randomly
            PreparedStatement getTestStmt = readConnections.get(ThreadLocalRandom.current().nextInt(0, readConnections.size()));
            getTestStmt.setLong(1, identifierLong);
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
            JSONArray itemsJSON = new JSONArray();

            while (testResult.next()) {
                //logger.info(metaData.toString());
                //logger.info(testResult.toString());

                JSONObject itemJSON = new JSONObject();
                for (int col = 1; col <= metaData.getColumnCount(); col++) {
                    String label = metaData.getColumnLabel(col);
                    logger.info("METADATA: " +label);
                    if (firstRow && Arrays.asList(topFieldNames).contains(label.toUpperCase())) {
                        Object val = testResult.getObject(col);
                        jsonObj.put(label.toLowerCase(), val);
                    }
                    // All Rows add to the Items array - this is a simple JSON structure
                    // Wiith just one top level array of objects

                    if (Arrays.asList(itemFieldNames).contains(label.toUpperCase())) {
                        Object val = testResult.getObject(col);
                        itemJSON.put(label.toLowerCase(), val);
                    }
                }
                /* If our item isnt blank add it to the items JSONArray */
                if (itemJSON.optInt("rfrid", -1) != -1) {
                    itemsJSON.put(itemJSON);
                }
                firstRow = false;
            }
            jsonObj.put("testitems", itemsJSON); 

            testResult.close();

            return jsonObj.toString();
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.toString());

        logger.info(jsonObj.toString(2));
        System.exit(0);
        }
        return jsonObj.toString();
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
        try {

            String countIdSQL = "SELECT COUNT(VEHICLEID) AS C FROM TESTRESULT";
            Statement countStatement = connection.createStatement();
            ResultSet countResult = countStatement.executeQuery(countIdSQL);

            if (!countResult.next()) {
                logger.error("Count Failed");
                return new long[0];
            }
            int nDocs = countResult.getInt("C");
            countResult.close();

            logger.info(String.format("TESTRESULT TABLE HAS %d rows", nDocs));
            int idx = 0;

            long[] vehicleids = new long[nDocs];

            String getIdSQL = "SELECT VEHICLEID FROM TESTRESULT  ";
            Statement getIdStatement = connection.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,
                    java.sql.ResultSet.CONCUR_READ_ONLY);

            if (isMySQL) {
                getIdStatement.setFetchSize(Integer.MIN_VALUE);
            } else {
                getIdStatement.setFetchSize(5000);
            }
            ResultSet idResults = getIdStatement.executeQuery(getIdSQL);
            // Only read enough to fill array
            for (idx = 0; idx < vehicleids.length && idResults.next(); idx++) {
                Long l = idResults.getLong("VEHICLEID");
                if(idx == 0) { logger.info("Example Vehicle ID: " + l);}
                vehicleids[idx] = l;
            }
            idResults.close();
            return vehicleids;
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage());
            System.exit(1);
        }
        return new long[0];
    }
}
