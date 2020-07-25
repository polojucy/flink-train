package org.example.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * java实现word count程序
 */
public class WordCountStreamingJob {

    public static void main(String[] args) throws Exception {
        // 1) 获取env
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2) 获取data 使用 `nc -lk 9999` 开启socket
        DataStreamSource<String> text = env.socketTextStream("localhost",9999);
        // 3) 处理数据  flatMap、groupBy、sum
        text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        String[] tokens = value.toLowerCase().split(",");
                        for (String token : tokens) {
                            if (token.length() > 0) {
                                collector.collect(new Tuple2<>(token, 1));
                            }
                        }
                    }
                })
                .keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1).print();

        env.execute("WordCountStreamingJob");
    }
}
