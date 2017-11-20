package com.example;

import lombok.val;
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
import java.util.Optional;

/**
 * @author Podshivalov N.A.
 * @since 18.11.2017.
 */
public class TransactionRecipientCounter extends BaseBasicBolt {
    private Map<String, Integer> transactionPerRecipient = new HashMap<>();

    private void countTransaction(String recipient){
        val current = Optional.ofNullable(transactionPerRecipient.get(recipient))
                .orElse(0);
        transactionPerRecipient.put(recipient, current);

    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        val recipient = tuple.getString(0);
        countTransaction(recipient);
        collector.emit(new Values(recipient, transactionPerRecipient.get(recipient)));
    }

    @Override
    public void cleanup() {
        transactionPerRecipient.forEach((recipient , num) -> System.out.println("Number of transaction is " + num + " for recipient " + recipient));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("recipient", "num"));
    }
}
