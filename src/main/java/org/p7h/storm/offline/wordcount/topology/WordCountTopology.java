package org.p7h.storm.offline.wordcount.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import org.p7h.storm.offline.wordcount.bolts.SplitSentenceBolt;
import org.p7h.storm.offline.wordcount.bolts.WordCountBolt;
import org.p7h.storm.offline.wordcount.spouts.RandomSentenceSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Orchestrates the elements and forms a Topology to count the words present in Tweets.
 *
 * @author - Prashanth Babu
 */
public final class WordCountTopology {
	private static final Logger LOGGER = LoggerFactory.getLogger(WordCountTopology.class);
	private static final String TOPOLOGY_NAME = "WordCount";

	public static final void main(final String[] args) {
		try {
			final Config config = new Config();
			config.setMessageTimeoutSecs(120);
			config.setDebug(false);

			final TopologyBuilder topologyBuilder = new TopologyBuilder();
			topologyBuilder.setSpout("randomsentencespout", new RandomSentenceSpout());
			topologyBuilder.setBolt("splitsentencebolt", new SplitSentenceBolt())
					.shuffleGrouping("randomsentencespout");
			topologyBuilder.setBolt("wordcountbolt", new WordCountBolt())
					.fieldsGrouping("splitsentencebolt", new Fields("word"));

			//Submit it to the cluster, or submit it locally
			if (null != args && 0 < args.length) {
				config.setNumWorkers(3);
				StormSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
			} else {
				config.setMaxTaskParallelism(10);
				final LocalCluster localCluster = new LocalCluster();
				localCluster.submitTopology(TOPOLOGY_NAME, config, topologyBuilder.createTopology());
				//Run this topology for 120 seconds so that we can complete processing of decent # of tweets.
				Utils.sleep(120 * 1000);

				LOGGER.info("Shutting down the cluster...");
				localCluster.killTopology(TOPOLOGY_NAME);
				localCluster.shutdown();
			}
		} catch (final AlreadyAliveException | InvalidTopologyException exception) {
			//Deliberate no op; not required actually.
			//exception.printStackTrace();
		} catch (final Exception exception) {
			//Deliberate no op; not required actually.
			//exception.printStackTrace();
		}
		LOGGER.info("\n\n\n\t\t*****Please clean your temp folder \"{}\" now!!!*****", System.getProperty("java.io.tmpdir"));
	}
}