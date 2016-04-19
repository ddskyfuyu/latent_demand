package com.bfd.userprofile.utils.cmd;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class CommadLineAddResource {
	private static Options options = new Options();
	public static void addResource(String[] args){
		Option o = new Option("t", "table", true,
				"table to import into (must exist)");
		o.setArgName("table-name");
		o.setRequired(true);	
		options.addOption(o);
	}
}
