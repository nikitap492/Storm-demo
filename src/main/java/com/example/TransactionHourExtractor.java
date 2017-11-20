package com.example;

import lombok.val;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.time.LocalTime;
import java.util.Vector;

/**
 * @author Podshivalov N.A.
 * @since 20.11.2017.
 */
public class TransactionHourExtractor extends BaseBasicBolt {

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        val hour = ((LocalTime) tuple.getValue(2)).getHour();
        collector.emit(new Values( hour, tuple.getValue(3)));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields( "hour", "amount"));
    }
}
