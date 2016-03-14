/**
 * This topology used for watching SegmentFault's haproxy log.
 */
package com.segmentfault.storm;

import storm.kafka.*;

import java.util.ArrayList;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/**
 * @author tairy
 * @date 2016-03-13
 * @version 0.0.1
 */
public class SampleTopology 
{
    public static class PrinterBolt extends BaseBasicBolt {
        /**
         * serialVersionUID
         */
        private static final long serialVersionUID = 3179969158010282366L;

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }

        public void execute(Tuple tuple, BasicOutputCollector collector) {
            System.out.println(tuple.toString());
        }
    }

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
