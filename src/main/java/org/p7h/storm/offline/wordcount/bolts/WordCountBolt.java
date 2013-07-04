package org.p7h.storm.offline.wordcount.bolts;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created with IntelliJ IDEA.
 * User: 078831
 * Date: 7/5/13
 * Time: 12:47 AM
 * To change this template use File | Settings | File Templates.
 */
public final class WordCountBolt extends BaseBasicBolt {
	private static final Logger LOGGER = LoggerFactory.getLogger(WordCountBolt.class);
	private static final long serialVersionUID = -7958498892723043354L;
	final Map<String, Integer> wordCountTracketMap = new HashMap<>();
	private Stopwatch stopwatch = null;

	@Override
	public void prepare(final Map stormConf, final TopologyContext context) {
		this.stopwatch = new Stopwatch();
		this.stopwatch.start();
	}

	@Override
	public final void execute(final Tuple input, final BasicOutputCollector collector) {
		//final String word = input.getString(0);
		final String word = (String) input.getValueByField("word");
		Integer count = this.wordCountTracketMap.get(word);
		count = (count == null) ? 1 : count + 1;
		this.wordCountTracketMap.put(word, count);

		if (5 < this.stopwatch.elapsed(TimeUnit.SECONDS)) {
			logWordCount();
			this.stopwatch.reset();
			this.stopwatch.start();
		}
	}

	private void logWordCount() {
		final StringBuilder wordCountLog = new StringBuilder();
		int i = 0;
		for (final String key : this.wordCountTracketMap.keySet()) {
			if (3 < key.length()) {
				i++;
				if (0 != (i % 4)) {
					wordCountLog
							.append(String.format("%15s", key))
							.append(": ")
							.append(String.format("%-3d", this.wordCountTracketMap.get(key)))
							.append("\t");
				} else {
					wordCountLog.append("\n");
				}
			}
		}
		LOGGER.info("\n\n{}\n{}", new Date(), wordCountLog.toString());
	}

	@Override
	public final void declareOutputFields(final OutputFieldsDeclarer outputFieldsDeclarer) {
		//no-op
	}
}