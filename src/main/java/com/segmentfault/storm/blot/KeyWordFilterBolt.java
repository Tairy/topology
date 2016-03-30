package com.segmentfault.storm.blot;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/**
 * Created by tairy on 30/3/2016.
 */
public class KeyWordFilterBolt extends BaseBasicBolt{
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String[] splitedLine = tuple.toString().split("\\s+");
//        for (String s : splitedLine) {
//            System.out.println(s);
//        }
        System.out.println(splitedLine[2] + "tairy-test");
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
