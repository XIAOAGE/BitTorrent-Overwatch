package bt_overwatch.bolt;

import java.util.Map;

import bt_overwatch.Topology;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SinkTypeBolt extends BaseRichBolt {


	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	

	public void execute(Tuple tuple) {
		String value = tuple.getString(0);
		System.out.println("Received in SinkType bolt : "+value);
		int index = value.indexOf(" ");
		if (index == -1)
			return;
		String type = value.substring(0,index);
		System.out.println("Type : "+type);
		value = value.substring(index);
		if (type.equals("mongo")) {
			collector.emit(Topology.MONGODB_STREAM,new Values(type,value));
			System.out.println("Emitted : "+value);
		}
		collector.ack(tuple);	
	}


	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(Topology.MONGODB_STREAM, new Fields( "sinkType","content" ));
	}

}