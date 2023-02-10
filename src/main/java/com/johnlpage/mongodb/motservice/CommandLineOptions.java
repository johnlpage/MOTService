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
	private int nThreads = 64;
	private int testSeconds = 600;
	private String destURI = null;
	private int readRatio = 80;
	private int createRatio = 15;
	private int updateRatio = 5;
	
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
		cliopt.addOption("t", "threads", true, String.format("Number of test threads (default %d)",nThreads));
		cliopt.addOption("s", "seconds", true, String.format("Number of seconds to run the test for (default %d)",testSeconds));
		cliopt.addOption("c", "createratio", true, String.format("Ratio of create operations (default %d)",createRatio));
		cliopt.addOption("r", "readratio", true, String.format("Ratio of read operations (default %d)",readRatio));
		cliopt.addOption("m", "updateratio", true, String.format("Ratio of update/modify operations (default %d)",updateRatio));
	
		CommandLine cmd = parser.parse(cliopt, args);

		if (cmd.hasOption("u")) {
			databaseURI = cmd.getOptionValue("u");
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

		if (cmd.hasOption("t")) {
			nThreads = Integer.parseInt(cmd.getOptionValue("t"));
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
		return nThreads;
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
