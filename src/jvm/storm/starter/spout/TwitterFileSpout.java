package storm.starter.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.util.Map;
import java.util.Random;

public class RandomSentenceSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;
    Random _rand;    
    BufferedReader _br;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		FileInputStream fstream = new FileInputStream("2012-07-28-19.txt");
	    DataInputStream in = new DataInputStream(fstream);
	    _br = new BufferedReader(new InputStreamReader(in));
		
    }

    @Override
    public void nextTuple() {
        Utils.sleep(100);
		_collector.emit(new Values(_br.readLine()));
    }        

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("fullTweet"));
    }
}