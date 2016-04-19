package com.bfd.userprofile.utils.cmd;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;


public final class CommandLineUtil {
  
  private CommandLineUtil() { 
  }
  
  public static CommandLine parseArgs(String[] args) {
		Options options = new Options();
		Option o = new Option("t", "table", true,
				"table to import into (must exist)");
		o.setArgName("table-name");
		o.setRequired(true);	
		options.addOption(o);

		o = new Option("c", "column", true,
				"column to store row data into (must exist)");
		o.setArgName("family:qualifier");
		o.setRequired(true);
		options.addOption(o);

		o = new Option("i", "input", true, "the directory or file to read from");
		o.setArgName("path-in-HDFS");
		o.setRequired(true);
		options.addOption(o);
		options.addOption("d", "debug", false, "switch on DEBUG log level");

		CommandLineParser parser = new PosixParser();
		CommandLine cmd = null;

		try {
			cmd = parser.parse(options, args);
		} catch (ParseException e) {
			System.err.println("ERROR: " + e.getMessage() + "\n");
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("", options, true);
			System.exit(-1);
		}

		return cmd;
	}

}

