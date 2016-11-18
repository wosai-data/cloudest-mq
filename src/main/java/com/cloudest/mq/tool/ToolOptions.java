package com.cloudest.mq.tool;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class ToolOptions {
    private Options options = new Options();
    private CommandLineParser parser = new DefaultParser();
    private HelpFormatter formatter = new HelpFormatter();
    private CommandLine cmd;

    public void addBrokers() {
        options.addOption(Option.builder("B")
                          .hasArg()
                          .argName("broker-list")
                          .longOpt("brokers")
                          .required(true)
                          .valueSeparator(',')
                          .desc("kafka brokers list").build());
    }

    public void add(Option option) {
        options.addOption(option);
    }

    public void add(OptionGroup group) {
        options.addOptionGroup(group);
    }

    public boolean parse(String[] args) {
        try {
            cmd = parser.parse(options, args);
            return true;
        }catch(ParseException ex) {
            System.out.println(ex.getMessage());
            formatter.printHelp("tool", options, true);
            return false;
        }

    }
    public String get(String opt) {
        return cmd.getOptionValue(opt);
    }
    public String[] getMulti(String opt) {
        return cmd.getOptionValues(opt);
    }
    public boolean has(String opt) {
        return cmd.hasOption(opt);
    }
    public String getBrokers() {
        return get("brokers");
    }
}
