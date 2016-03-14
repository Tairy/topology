/**
 * This topology is a sample example for reading message from kafka.
 */
package com.segmentfault.storm;

import storm.kafka.*;

import java.util.ArrayList;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;

import com.segmentfault.storm.blot.PrinterBolt;


/**
 * @author tairy
 * @version 0.0.1
 */
public class SampleTopology 
{
    /**
     * @param args
     * @throws Exception
     */
    public static void main(String args[]) throws Exception
    {
        String zookeeperHost = "docker.tairy.me:2181";
        ZkHosts zkHosts = new ZkHosts(zookeeperHost);
        SpoutConfig spoutConfig = new SpoutConfig(zkHosts, "sf-log", "/kafkastorm", "id");
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConfig.zkServers = new ArrayList<String>() {{  
            add("docker.tairy.me");  
        }};  
        spoutConfig.zkPort = 2181;
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("sf-log", kafkaSpout,  1);
        builder.setBolt("print", new PrinterBolt()).shuffleGrouping("sf-log");
        LocalCluster cluster = new LocalCluster();
        Config config = new Config();
        cluster.submitTopology("kafka-test", config, builder.createTopology());

        Thread.sleep(6000);
    }
}
