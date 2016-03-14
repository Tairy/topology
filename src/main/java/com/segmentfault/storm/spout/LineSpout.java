package com.segmentfault.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by tairy on 14/3/2016.
 */
public class LineSpout extends BaseRichSpout {
    private String fileName;
    private SpoutOutputCollector _collector;
    private BufferedReader reader;
    private AtomicLong linesRead;

    /**
     * Tell storm which fields are emited by the spout.
     * @param outputFieldsDeclarer
     */
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("line"));
    }

    /**
     * Prepare the spout, this method is called once when the topology is submited.
     * @param map
     * @param topologyContext
     * @param spoutOutputCollector
     */
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        linesRead = new AtomicLong(0);
        _collector = spoutOutputCollector;

        try {
            fileName = (String) map.get("linespout.file");
            reader = new BufferedReader(new FileReader(fileName));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Strom will call this method repeatedly pull tuples from spout.
     */
    public void nextTuple() {
        try {
            String line = reader.readLine();
            if(line != null) {
                long id = linesRead.incrementAndGet();
                _collector.emit(new Values(line), id);
            } else {
                System.out.println("Finlish reading file, " + linesRead.get() + "lines read");
                Thread.sleep(10000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Storm call this method when tuples are acked.
     * @param id
     */
    public void ack(Object id) {

    }

    /**
     * Storm will call this method when tuples fail to process downstream.
     * @param id
     */
    public void fail(Object id) {
        System.err.println("Failed line number" + id);
    }
}
