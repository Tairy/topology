package com.segmentfault.storm.blot;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/**
 * Created by tairy on 14/3/2016.
 */
public class PrinterBolt extends BaseBasicBolt {
    /**
     * serialVersionUID
     */
    private static final long serialVersionUID = 3179969158010282366L;

    /**
     * @param declarer
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    /**
     * @param tuple
     * @param collector
     */
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        System.out.println(tuple.toString());
    }
}
