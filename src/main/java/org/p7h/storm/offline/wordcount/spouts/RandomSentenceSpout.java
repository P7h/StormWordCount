package org.p7h.storm.offline.wordcount.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

public final class RandomSentenceSpout extends BaseRichSpout {
    private static final long serialVersionUID = -3830075232094686690L;
    private SpoutOutputCollector _collector;
	private Random _rand;

	@Override
	public final void open(final Map conf, final TopologyContext context, final SpoutOutputCollector collector) {
		_collector = collector;
		_rand = new Random();
	}

	@Override
	public final void nextTuple() {
		Utils.sleep(1000);
		final String sentence = SENTENCES[_rand.nextInt(SENTENCES.length)];
		_collector.emit(new Values(sentence));
	}

	@Override
	public final void ack(final Object id) {
	}

	@Override
	public final void fail(final Object id) {
	}

	@Override
	public final void declareOutputFields(final OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}


	private final static String[] SENTENCES = new String[]{
          "thanks, nancy",
          "hello, everyone, and thank you for joining us",
          "we have a lot of news to share with you today about the details of our march quarter, as well as a significant increase to our capital return program",
          "first, i'd like to talk about our business and the road ahead",
          "we're now half way through our fiscal 2013, and we've accomplished a tremendous amount",
          "we've introduced and ramped production of an unprecedented number of new products, and we've set many new sales records",
          "our revenue for the first half was over $98 billion, and our net income was over $22 billion",
          "during that time we sold 85 million iphones and 42 million ipads",
          "these are very, very large numbers, unimaginable even to us just a few years ago",
          "despite producing results that met or beat our guidance as we have done consistently, we know they didn't meet everyone's expectations",
          "and though we've achieved incredible scale and financial success, we acknowledge that our growth rate has slowed and our margins have decreased from the exceptionally high level we experienced in 2012",
          "our revenues grew about $13 billion in the first half of the fiscal year",
          "even though that's like adding the total first half revenue of five fortune 500 companies, our average weekly growth slowed to 19% and our gross margins are closer to the levels of a few years ago",
          "our fiscal 2012 results were incredibly strong and that's making comparisons very difficult this year",
          "last year our business benefited from both high growth and demand for our products and a corresponding growth in channel inventories along with a richer mix of higher gross margin products, a more favorable foreign currency environment and historically low costs",
          "these compares are made further challenging until we anniversary the launch of the ipad mini, which as you know we strategically priced at a lower margin",
          "as peter will discuss, we are guiding to flat revenues year-over-year for the june quarter along with a slight sequential decline in gross margins.",
          "the decline in apple's stock price over the last couple of quarters has been very frustrating to all of us",
          "but apple remains very strong and will continue to do what we do best",
          "we can't control items such as exchange rates and world economies and even certain cost pressures",
          "but the most important objective for apple will always be creating innovative products and that is directly within our control",
          "we will continue to focus on the long-term and we remain very optimistic about our future",
          "we are participating on large and growing markets",
          "we see great opportunities in front of us, particularly given the long-term prospects of the smartphone and tablet market, the strength of our incredible ecosystem which we plan to continue to augment with new services, our plans for expanded distribution and the potential of exciting new product categories",
          "take the smartphone market, for example",
          "idc estimates that this market will double between 2012 and 2016 to an incredible 1.4 billion units annually and gartner estimates that the tablet market is growing at an even faster rate from 125 million units in 2012 to a projected 375 million by 2016",
          "our teams are hard at work on some amazing new hardware, software and services that we can't wait to introduce this fall and throughout 2014",
          "we continue to be very confident in our future product plans",
          "apple has many distinct and unique advantages as the only company in the industry with world-class skills in hardware, software and services",
          "we have the strongest ecosystem in the industry with app stores in 155 countries, itunes music stores in 119 countries, hundreds of millions of icloud users around the world; and most importantly, the highest loyalty and customer satisfaction rates in the business",
          "and of course, we have a tremendous culture of innovation with a relentless focus on making the world's best products that change people's lives",
          "this is the same culture and company that brought the world the iphone and ipad and we've got a lot more surprises in the works",
          "a little over a year ago, we announced a plan to return $45 billion to shareholders over three years",
          "since we began paying dividends last august and began share buybacks last october, we've already returned $10 billion under that program",
          "while we continue to generate cash in excess of our needs to operate the business, invest in our future and maintain flexibility to take advantage of strategic opportunities, we remain firmly committed to our objective of delivering attractive returns to shareholders through both our business performance and the return of capital",
          "so, today, we are announcing an aggressive plan that more than doubles the size of the capital return program we announced last year to a total of $100 billion by the end of calendar year 2015",
          "the vast majority of our incremental cash return will be in the form of share repurchases",
          "as the board and management team deliberated among the various alternatives to returning cash, we concluded that investing in apple was the best",
          "in addition to share repurchases, we are increasing our current dividend by 15% to further appeal to investors seeking yield",
          "and as part of our updated program, we will access the debt market.",
          "peter will provide more details about all of this in a moment",
          "we appreciate the input that so many of our shareholders have provided us on how best to deploy our cash",
          "we will review our cash allocation strategy each year, and we will continue to invest confidently in the business to bring great new products to market, strategically deploy capital in our supply chain, our retail stores and our infrastructure, and make acquisitions to expand our capabilities",
          "we will be disciplined in what we do, but we will not underinvest",
          "i'd now like to turn the call over to peter to discuss the details of the march quarter."
	};


}