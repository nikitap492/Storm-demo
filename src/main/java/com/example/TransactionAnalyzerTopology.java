package com.example;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * @author Podshivalov N.A.
 * @since 15.11.2017.
 */
public class TransactionAnalyzerTopology {
    public static void main(String[] args) throws Exception{
        Config config = new Config();
        config.setDebug(true);
        config.registerMetricsConsumer(org.apache.storm.metric.LoggingMetricsConsumer.class);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("transaction-spout", new TransactionSpout(), 3);


        builder.setBolt("transaction-counter-bolt", new TransactionCounter())
                .shuffleGrouping("transaction-spout");

        builder.setBolt("transaction-hour-extractor", new TransactionHourExtractor())
                .shuffleGrouping("transaction-spout");

        builder.setBolt("transaction-amount-per-hour-bolt", new TransactionAmountPerHoursCounter())
                .fieldsGrouping("transaction-hour-extractor", new Fields("hour"));

        builder.setBolt("top-payer-analyzer-bolt", new TopPayerAnalyzer())
                .fieldsGrouping("transaction-spout", new Fields("payer"));

        builder.setBolt("transaction-recipient-analyzer-bolt", new TransactionRecipientCounter())
                .fieldsGrouping("transaction-spout", new Fields("recipient"));

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("LogAnalyserStorm", config, builder.createTopology());

        Thread.sleep(100000);
        cluster.shutdown();
    }
}
