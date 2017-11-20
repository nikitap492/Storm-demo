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

import java.math.BigDecimal;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Vector;

/**
 * @author Podshivalov N.A.
 * @since 18.11.2017.
 */
public class TransactionAmountPerHoursCounter extends BaseBasicBolt {
    private Map<Integer, BigDecimal> amountPerHour = new HashMap<>(12);

    private void addToContainer(Integer hour, BigDecimal amount){
        val previous = Optional.ofNullable(amountPerHour.get(hour))
                .orElse(BigDecimal.ZERO);

        amountPerHour.put(hour, previous.add(amount));
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        val amount = (BigDecimal) tuple.getValue(1);
        val hour = tuple.getInteger(0);
        addToContainer(hour, amount);
        collector.emit(new Values(hour, amountPerHour.get(hour)));
    }

    @Override
    public void cleanup() {
        amountPerHour.forEach((hour, amount) -> System.out.println("Amount: " + amount + " at " + hour ));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("hour", "amount"));
    }

}
