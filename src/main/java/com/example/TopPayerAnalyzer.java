package com.example;

import lombok.val;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author Podshivalov N.A.
 * @since 18.11.2017.
 */
public class TopPayerAnalyzer implements IRichBolt {
    private Map<String, Integer> transactionPerPayer;
    private OutputCollector collector;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.transactionPerPayer = new HashMap<>();
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        val payer = tuple.getString(0);
        countTransaction(payer);
        collector.ack(tuple);
    }

    private void countTransaction(String payer){
        val current = Optional.ofNullable(transactionPerPayer.get(payer))
                .orElse(0);
        transactionPerPayer.put(payer, current + 1);

    }

    @Override
    public void cleanup() {
        transactionPerPayer.keySet().stream()
                .sorted((p1, p2) -> transactionPerPayer.get(p2).compareTo(transactionPerPayer.get(p1)))
                .limit(10)
                .forEach((payer) -> System.out.println("Payer : " + payer));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("top-payers"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
