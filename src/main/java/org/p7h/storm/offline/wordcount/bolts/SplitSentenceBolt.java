package org.p7h.storm.offline.wordcount.bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public final class SplitSentenceBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 3077170245322026396L;

	@Override
	public final void execute(final Tuple input, final BasicOutputCollector collector) {
		//final String sentence = input.getString(0);
		final String sentence = (String) input.getValueByField("sentence");
		for (final String word : sentence.split(" ")) {
			collector.emit(new Values(word));
		}
	}

	@Override
	public final void declareOutputFields(final OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("word"));
	}
}