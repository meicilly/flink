package com.meicilly.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCountBatch {
    public static void main(String[] args) throws Exception {
        //MC TODO:创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //MC TODO:读取数据
        DataSource<String> lineDS = env.readTextFile("data/word.txt");
        //MC TODO:切分转换成 (word,1)
        FlatMapOperator<String, Tuple2<String, Integer>> wordAndOne = lineDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
                //MC TODO:按照空格 切分单词
                String[] words = value.split(" ");
                //MC TODO:将单词转换为 (word,1)
                for (String word : words) {
                    Tuple2<String, Integer> wordTuple2 = Tuple2.of(word, 1);
                    // TODO: 2023/8/30 使用Collector向下游发送数据
                    collector.collect(wordTuple2);
                }
            }
        });
        //MC TODO:按照word分组
        UnsortedGrouping<Tuple2<String, Integer>> wordAndOneGroupBy = wordAndOne.groupBy(0);
        //MC TODO:各分组聚合
        AggregateOperator<Tuple2<String, Integer>> sum = wordAndOneGroupBy.sum(1);//1表示的是位置
        //MC TODO:输出
        sum.print();
    }
}
