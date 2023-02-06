package com.johnlpage.mongodb.motservice;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;

/* 
* This is JDBC and I've developed it with MySQL as that's what the
 * sameple data documentation sugget and MySQL 8 isn't bad at all
 * But I'm sure you could make it work with Postgres too
 */

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSetMetaData;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class JDBCFetcher implements MOTFetcherInterface {

    private Logger logger;
    boolean initialised = false;
    boolean isMySQL = false;
    private Connection connection = null;
    PreparedStatement getTestStmt = null;
    long threadId = 0;
   

    private final String getlatestByVehicleSQL = "select "+
    "tr.*, "+
    "ft.FUEL_TYPE, "+
    "tt.TESTTYPE AS TYPENAME, "+
    "to2.RESULT, "+
    "ti.RFRID, ti.RFRTYPE, "+
    "fl.LAT,fl.LONGITUDINAL,fl.VERTICAL, "+
    "tid.MINORITEM,tid.RFRDESC,tid.RFRLOCMARKER,tid.RFRINSPMANDESC,tid.RFRADVISORYTEXT,tid.TSTITMSETSECID, "+
    "b.ITEMNAME AS LEVEL1, "+
    "c.ITEMNAME AS LEVEL2, "+
    "d.ITEMNAME AS LEVEL3, "+
    "e.ITEMNAME AS LEVEL4, "+
    "f.ITEMNAME AS LEVEL5  "+
    "from TESTRESULT tr "+
    "LEFT JOIN TESTITEM ti  on ti.TESTID = tr.TESTID "+
    "LEFT JOIN FUEL_TYPES ft on ft.TYPECODE = tr.FUELTYPE "+
    "LEFT JOIN TEST_TYPES tt on tt.TYPECODE  = tr.TESTTYPE "+
    "LEFT JOIN TEST_OUTCOME to2 on to2.RESULTCODE  = tr.TESTRESULT "+
    "LEFT JOIN FAILURE_LOCATION fl on TI.LOCATIONID = fl.FAILURELOCATIONID "+
    "LEFT JOIN TESTITEM_DETAIL AS tid ON ti.RFRID = tid.RFRID AND tid.TESTCLASSID = tr.TESTCLASSID "+
    "LEFT JOIN TESTITEM_GROUP AS b ON tid.TSTITMID = b.TSTITMID AND tid.TESTCLASSID = b.TESTCLASSID "+
    "LEFT JOIN TESTITEM_GROUP AS c ON b.PARENTID = c.TSTITMID AND b.TESTCLASSID = c.TESTCLASSID "+
    "LEFT JOIN TESTITEM_GROUP AS d ON c.PARENTID = d.TSTITMID AND c.TESTCLASSID = d.TESTCLASSID "+
    "LEFT JOIN TESTITEM_GROUP AS e ON d.PARENTID = e.TSTITMID AND d.TESTCLASSID = e.TESTCLASSID "+
    "LEFT JOIN TESTITEM_GROUP AS f ON e.PARENTID = f.TSTITMID AND e.TESTCLASSID = f.TESTCLASSID "+
    "WHERE  tr.TESTID = (SELECT TESTID FROM TESTRESULT WHERE VEHICLEID=? LIMIT 1)";



             

    JDBCFetcher(String URI) {
        logger = LoggerFactory.getLogger(JDBCFetcher.class);

        try {
            if (URI.contains("mysql://")) {
                isMySQL = true;
                logger.info("RDBMS is MySQL");
            }
            connection = DriverManager.getConnection(URI);
           
            getTestStmt = connection.prepareStatement(getlatestByVehicleSQL, java.sql.ResultSet.TYPE_FORWARD_ONLY,
                    java.sql.ResultSet.CONCUR_READ_ONLY);
            
            this.threadId = Thread.currentThread().getId();

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


   
    public String getMOTResultInJSON(String identifier)
    {
        long identifierLong;
        JSONObject jsonObj =  new JSONObject();
        //Check we aren't a new thread - if we are we need a new conneciton.
        try {
            identifierLong = Long.valueOf(identifier);
            
            getTestStmt.setLong(1,identifierLong);
            ResultSet testResult = getTestStmt.executeQuery();
            ResultSetMetaData metaData  = testResult.getMetaData();
            //Create JSON from a set of Rows
            String[] topFieldNames = { "TESTID","VEHICLEID","TESTDATE","TESTCLASSID","TYPENAME",
            "TESTMILEAGE","POSTCODEREGION","MAKE","MODEL","COLOUR","FUEL_TYPE","CYLCPCTY","FIRSTUSEDATE",
            "RESULT"};

            String[] itemFieldNames = { "RFRID","RFRTYPE","LAT","LONGITUDINAL","VERTICAL","MINORITEM","RFRDESC","RFRLOCMARKER",
            "RFRINSPMANDESC","RFRADVISORYTEXT","LEVEL1","LEVEL2","LEVEL3","LEVEL4","LEVEL5"};

            boolean firstRow = true;
            JSONArray itemsJSON = new JSONArray();

            while(testResult.next()) {
                JSONObject itemJSON = new JSONObject();
               for(int col=1;col<=metaData.getColumnCount();col++) {
                String label = metaData.getColumnLabel(col);
                if(firstRow && Arrays.asList(topFieldNames).contains(label)) {
                    Object val = testResult.getObject(col);
                    if(val != null ) {
                        jsonObj.put(label.toLowerCase(),val);
                    }
                }
                //All Rows add to the Items array - this is a simple JSON structure
                //Wiith just one top level array of objects
              
                if(Arrays.asList(itemFieldNames).contains(label)) {
                    Object val = testResult.getObject(col);
                    if(val != null ) {
                        itemJSON.put(label.toLowerCase(),val);
                    }
                }
            }
            /* If our item isnt blank add it to the items JSONArray */
            if(itemJSON.optInt("rfrid",-1) != -1) {
                itemsJSON.put(itemJSON);
            }
            firstRow = false;
        }
            jsonObj.put("testitems",itemsJSON);
           
            testResult.close();
          
            return jsonObj.toString();
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.toString());
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

            String getIdSQL = "SELECT VEHICLEID FROM TESTRESULT";
            Statement getIdStatement = connection.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,
                    java.sql.ResultSet.CONCUR_READ_ONLY);

            if (isMySQL) {
                getIdStatement.setFetchSize(Integer.MIN_VALUE);
            }
            ResultSet idResults = getIdStatement.executeQuery(getIdSQL);
            // Only read enough to fill array
            for (idx = 0; idx < vehicleids.length && idResults.next(); idx++) {
                Long l  = idResults.getLong("VEHICLEID");
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

