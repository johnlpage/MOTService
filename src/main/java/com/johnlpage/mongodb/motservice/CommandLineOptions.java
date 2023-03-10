package com.johnlpage.mongodb.motservice;

import org.apache.commons.cli.CommandLine;

import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommandLineOptions {
	private boolean helpOnly = false;
	private String databaseURI = null;
	private boolean runWebservice = false;

	private int testSeconds = 600;
	private String destURI = null;
	private int readRatio = 80;
	private int createRatio = 15;
	private int updateRatio = 5;
	private boolean readReplicas = false;
	private String replicaList = null;
	
	
	public String getReplicaList() {
		return replicaList;
	}


	public boolean isReadReplicas() {
		return readReplicas;
	}


	public int getReadRatio() {
		return readRatio;
	}


	public int getCreateRatio() {
		return createRatio;
	}

	public int getUpdateRatio() {
		return updateRatio;
	}

	public String getDestURI() {
		return destURI;
	}

	public CommandLineOptions(String[] args) throws ParseException {
		Logger logger = LoggerFactory.getLogger(CommandLineOptions.class);
		logger.info("Parsing Command Line");

		CommandLineParser parser = new DefaultParser();

		Options cliopt;
		cliopt = new Options();

		cliopt.addOption("h", "help", false, "Show Help");
		cliopt.addOption("u", "uri", true, "Database Connection String or URI");
		cliopt.addOption("d", "dest", true, "Destnation URIL for Miration to MongoDB");
		cliopt.addOption("w", "webservice", false, "Run an actual webservice");
		cliopt.addOption("s", "seconds", true, String.format("Number of seconds to run the test for (default %d)",testSeconds));
		cliopt.addOption("c", "createratio", true, String.format("Number of threads doing of create operations (default %d)",createRatio));
		cliopt.addOption("r", "readratio", true, String.format("Number of threads doing read operations (default %d)",readRatio));
		cliopt.addOption("m", "updateratio", true, String.format("Number of threads doing update/modify operations (default %d)",updateRatio));
		cliopt.addOption("x", "distributereads", true, "Include reading from replicas for RDBMS list URI's as comma seperated list");
		CommandLine cmd = parser.parse(cliopt, args);

		if (cmd.hasOption("u")) {
			databaseURI = cmd.getOptionValue("u");
		}
		
		if (cmd.hasOption("x")) {
			readReplicas = true;
			if( cmd.getOptionValue("x") != null)
			{
				replicaList =  cmd.getOptionValue("x");
			}
		}

		if (cmd.hasOption("d")) {
			destURI = cmd.getOptionValue("d");
		}

		if (cmd.hasOption("s")) {
			testSeconds = Integer.parseInt(cmd.getOptionValue("s"));
		}

		if (cmd.hasOption("c")) {
			createRatio = Integer.parseInt(cmd.getOptionValue("c"));
		}

		if (cmd.hasOption("r")) {
			readRatio = Integer.parseInt(cmd.getOptionValue("r"));
		}

		if (cmd.hasOption("m")) {
			updateRatio = Integer.parseInt(cmd.getOptionValue("m"));
		}


		if (cmd.hasOption("w")) {
			runWebservice = true;
		}

		if (cmd.hasOption("help")) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("MOTService", cliopt);
			helpOnly = true;
		}
	}

	public int getnThreads() {
		return readRatio+createRatio+updateRatio;
	}

	public int getTestLength() {
		return testSeconds;
	}

	public boolean isWebService()
	{
		return runWebservice;
	}
	public boolean isHelpOnly() {
		return helpOnly;
	}


	public String getURI() {
		return databaseURI;
	}
}
