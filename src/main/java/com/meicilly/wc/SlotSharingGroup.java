package com.meicilly.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 并行子任务和并行度
 * 当要处理的数据量非常大时，我们可以把一个算子操作，“复制”多份到多个节点，数据来了之后就可以到任意一个执行。这样依赖
 * 一个算子任务就会被拆分成了多个并行的“子任务”(subtasks),再将他们分发到不同节点 就真正实现了并行计算
 *
 *在Flink中，每一个算子(operator)可以包含一个或多个子任务(operator subtask),这些子任务在不同的线程、不同的物理机或不同的容器中完全独立执行
 * 一个特定算子的子任务(subtask)的个数被称之为其并行度(parallelism).
 */
public class SlotSharingGroup {
    public static void main(String[] args) throws Exception {
        // TODO 1. 创建执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // IDEA运行时，也可以看到webui，一般用于本地测试
        // 需要引入一个依赖 flink-runtime-web
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        // 在idea运行，不指定并行度，默认就是 电脑的 线程数
        env.setParallelism(1);



        // TODO 2. 读取数据： socket
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 7777);

        // TODO 3. 处理数据: 切换、转换、分组、聚合
        SingleOutputStreamOperator<Tuple2<String,Integer>> sum = socketDS
                .flatMap(
                        (String value, Collector<String> out) -> {
                            String[] words = value.split(" ");
                            for (String word : words) {
                                out.collect(word);
                            }
                        }
                )
                .returns(Types.STRING)
                .map(word -> Tuple2.of(word, 1)).slotSharingGroup("aaa")
                .returns(Types.TUPLE(Types.STRING,Types.INT))
                .keyBy(value -> value.f0)
                .sum(1);


        // TODO 4. 输出
        sum.print();

        // TODO 5. 执行
        env.execute();
    }
}

/**
 1、slot特点：
    1）均分隔离内存，不隔离cpu
    2）可以共享：
          同一个job中，不同算子的子任务 才可以共享 同一个slot，同时在运行的
          前提是，属于同一个 slot共享组，默认都是“default”

 2、slot数量 与 并行度 的关系
    1）slot是一种静态的概念，表示最大的并发上限
       并行度是一种动态的概念，表示 实际运行 占用了 几个

    2）要求： slot数量 >= job并行度（算子最大并行度），job才能运行
       TODO 注意：如果是yarn模式，动态申请
         --》 TODO 申请的TM数量 = job并行度 / 每个TM的slot数，向上取整
       比如session： 一开始 0个TaskManager，0个slot
         --》 提交一个job，并行度10
            --》 10/3,向上取整，申请4个tm，
            --》 使用10个slot，剩余2个slot



 */