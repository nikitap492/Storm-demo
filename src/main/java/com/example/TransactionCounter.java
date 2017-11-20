package com.example;

import com.sun.org.apache.xpath.internal.SourceTree;
import org.apache.storm.task.IBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Podshivalov N.A.
 * @since 15.11.2017.
 */
public class TransactionCounter extends BaseBasicBolt {
    private long counter = 0;

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        counter++;
        collector.emit(new Values(counter));
    }

    @Override
    public void cleanup() {
        System.out.println("Number of transactions: " + counter);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("number"));
    }
}