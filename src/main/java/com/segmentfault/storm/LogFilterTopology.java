package com.segmentfault.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import com.segmentfault.storm.blot.PrinterBolt;
import com.segmentfault.storm.spout.LineSpout;

/**
 * Created by tairy on 14/3/2016.
 */
public class LogFilterTopology
{
    /**
     * @param args
     * @throws Exception
     */
    public static void main(String args[]) throws Exception
    {
        String sourceFileName = "/Users/tairy/Documents/Working/log/delete-repeat.txt";
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("sf-log", new LineSpout(),  1);
        builder.setBolt("print", new PrinterBolt()).shuffleGrouping("sf-log");
        LocalCluster cluster = new LocalCluster();
        Config config = new Config();
        config.setDebug(true);
        config.put("linespout.file", sourceFileName);
        cluster.submitTopology("kafka-test", config, builder.createTopology());

        Thread.sleep(6000);
    }
}
