package com.johnlpage.mongodb.motservice;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.LogManager;
import static spark.Spark.*;

import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* This is a simple, webservice which returns the MOT Certificate for a given Vehicle */
/* Thge same as  https://www.gov.uk/check-mot-status - it's purpose is to compare the performance of
 * and RDBMS and a Document database for this tasks as well as aooling comparison of the code
 * Although it will include a micro webserver the actual performance comparison will
 * not include the serving via HTTPS, just the construciton of the payload
*/

public class MOTService {
	private static final String version = "0.0.1";
	private static CommandLineOptions options;
	private static MOTDataAccessInterface dal;

	public static void main(String[] args) {
		LogManager.getLogManager().reset();
		Logger logger = LoggerFactory.getLogger(MOTService.class);
		logger.info(String.format("MOTService version %s", version));

		try {
			options = new CommandLineOptions(args);
			if (options.isHelpOnly()) {
				return;
			}
		} catch (ParseException e) {
			System.err.println(e.getLocalizedMessage());
			System.exit(1);
		}

		if (options.getURI() == null) {
			logger.error("No Connection Details supplied, exiting.");
			System.err.println("You must supply database connection details with -u");
			System.exit(1);
		}

		if (options.getDestURI() != null) {
			logger.info("Data Migration Detected");
			Migrator migrator = new Migrator(options.getURI(), options.getDestURI());
			migrator.migrateData();
			return;
		} else if (options.getURI().startsWith("mongodb")) {
			logger.info("MongoDB URI Detected");
			dal = new MongoDBDataAccessLayer(options.getURI(),options.isReadReplicas());
			if (dal.initialised() == false) {
				logger.error("Could not connect to MongoDB");
				System.exit(1);
			}

		} else if (options.getURI().startsWith("jdbc")) {
			logger.info("JDBC Connection String Detected");
			dal = new JDBCDataAccessLayer(options.getURI(),options.getReplicaList());
			if (dal.initialised() == false) {
				logger.error("Could not connect to RDBMS");
				System.exit(1);
			}
		} else {
			logger.error("Unrecognised Connection Details supplied, exiting.");
			System.err.println("Supplied connection details must start with jdbc: or mongodb: (or mongodb+srv:)");
			System.exit(1);
		}

		// Now test retrieval speed OR start a webserver. The webserver
		// isn't for any good reason - and if you POST or PATCH it will allways
		// Ignore any JSON you send and update the last one read

		if (options.isWebService()) {
			get("/result/:vehicleid", (request, response) -> {
				return dal.getMOTResultInJSON(request.params(":vehicleid"));

			});
			post("/result/:vehicleid/:testid", (request, response) -> {
				return dal.createNewMOTResult(Long.parseLong(":testid"),Long.parseLong(":vehicleid"));
			});

			patch("/result/:vehicleid", (request, response) -> {
				return dal.updateMOTResult(Long.parseLong(":vehicleid"));
			});

		}

		else {

			// Get the set of unique Vehicle Identifiers so we can always choose
			// and Existing One
			logger.info(String.format("Testing for %d seconds with %d client threads.", options.getTestLength(),
					options.getnThreads()));
			logger.info(String.format("Ratio of Create/Read/Update is %d : %d : %d ", options.getCreateRatio(),
					options.getReadRatio(), options.getUpdateRatio()));
			logger.info(String.format("Allowing reads from Replicas? %s",options.isReadReplicas()?"Yes":"No"));
			logger.info("Preparing for test, this can take some time ....");
			dal.resetTestDatabase();

			long[] vehicleids = dal.getVehicleIdentifiers();
			logger.info(String.format("DB has %d vehicle ids", vehicleids.length));
			// Multi threaded testing of speed

			ExecutorService executorService = Executors.newFixedThreadPool(options.getnThreads());
			List<TestWorker> workers = new ArrayList<TestWorker>();

			int nWorkerThreads = options.getnThreads();

			for (int threadNo = 0; threadNo < nWorkerThreads; threadNo++) {

				// In MongoDB the database connection is thread safe, has a conneciton pool and
				// should be a Singleton.
				// In MySQL/JDBC this is very much not true and we need one per thread - however
				// out DAL also has some state
				// so we are having one DAL per thread - the MongoDB ones share the Connection
				// object as it's a static memeber

				if (options.getURI().startsWith("jdbc")) {
					dal = new JDBCDataAccessLayer(options.getURI(),options.getReplicaList());
				}

				if (options.getURI().startsWith("mongodb")) {
					dal = new MongoDBDataAccessLayer(options.getURI(),options.isReadReplicas());
				}

				TestWorker tw = new TestWorker(dal, vehicleids, options, threadNo);
				workers.add(tw);

			}
			logger.info("Ready...Steady...GO!");

			for (TestWorker tw : workers) {
				executorService.execute(tw); // calls run()
			}
			executorService.shutdown();
			try {
				logger.info("Waiting for all threads to complete");
				executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
			} catch (InterruptedException e) {
				logger.info(e.getMessage());
			}

		}
	}

}
