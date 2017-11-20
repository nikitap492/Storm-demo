package com.example;

import lombok.val;
import org.apache.storm.spout.ISpout;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.IBolt;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * @author Podshivalov N.A.
 * @since 18.11.2017.
 */
public class TransactionSpout extends BaseRichSpout {
    private static final int ENOUGH = 100;

    private SpoutOutputCollector collector;
    private TransactionGenerator generator;
    private int idx = 0;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.generator = new TransactionGenerator();
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        while (++idx < ENOUGH)
            generator.generate(100).stream()
                    .map(this::toValues)
                    .forEach(collector::emit);
    }

    private Values toValues(CardTransaction transaction){
        return new Values(transaction.getPayer(), transaction.getRecipient(),
                transaction.getCreatedAt().toLocalTime(), transaction.getAmount());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("payer", "recipient", "time", "amount"));
    }
}
