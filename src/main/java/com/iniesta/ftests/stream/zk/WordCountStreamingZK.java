package com.iniesta.ftests.stream.zk;

import java.io.IOException;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class WordCountStreamingZK {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStream<Tuple2<String, String>> ds = env.socketTextStream("elk", 9999).map(new ZKMapper());

		ds.print();

		env.execute("WordCount with connection from zookeeper in Streaming");
	}

	static class ZKMapper extends RichMapFunction<String, Tuple2<String, String>> {
		private static final long serialVersionUID = 8105251394747328689L;

		private static final String SEPARATOR_PATH = "/separator";

		public ZKMapper() {

		}

		private String regex;

		private ZooKeeper zk;

		private synchronized String getRegex() {
			System.out.println(this.hashCode());
			return regex;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			try {
				zk = ZookeeperConnection.getInstance().connect("elk:2181", 10000);
				final ZKMapper mapper = this;
				Stat stat = zk.exists(SEPARATOR_PATH, new FlinkWatcher(SEPARATOR_PATH, zk, new Callback() {
					@Override
					public void handle(String separator) {
						System.out.println("Change in the separator to: " + separator + " / " + mapper.hashCode());
						synchronized (regex) {
							regex = separator;
						}
					}
				}));
				if (stat != null) {
					System.out.println("Failure... no separator detected");
					byte[] data = zk.getData(SEPARATOR_PATH, true, stat);
					regex = new String(data);
				}
			} catch (IOException | InterruptedException | KeeperException e) {
				e.printStackTrace();
			} finally {
				System.out.println("Open called: " + zk);
			}
		}

		@Override
		public void close() throws Exception {
			zk.close();
		}

		@Override
		public Tuple2<String, String> map(String input) throws Exception {
			String regex = getRegex();
			if (regex != null) {
				String[] split = input.split(regex);
				if (split.length > 1) {
					return new Tuple2<String, String>(split[0], split[1]);
				} else {
					return new Tuple2<String, String>(split[0], "");
				}
			} else {
				return new Tuple2<String, String>("input", "");
			}
		}

	}

	static class FlinkWatcher implements Watcher {

		private ZooKeeper zk;
		private String path;
		private Callback callback;

		public FlinkWatcher(String path, ZooKeeper zk, Callback callback) {
			this.path = path;
			this.zk = zk;
			this.callback = callback;
		}

		@Override
		public void process(WatchedEvent event) {
			try {
				Stat stat = zk.exists(path, true);
				byte[] data = zk.getData(path, true, stat);
				callback.handle(new String(data));
			} catch (KeeperException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	static interface Callback {
		void handle(String separator);
	}
}
