package com.meicilly.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 集群角色
 * *1.客户端(Client)：代码由客户端获取并做转换 之后提交给JobManager
 * *2.JobManager就是Flink集群里的管事的 对作业进行中央调度管理 获取到执行的作业后会进一步处理转换 然后分发任务给众多的TaskManager
 * *3.TaskManger 就是真正干活的 数据的处理操作都是他们来做
 *
 * 部署模式
 * 1.会话模式
 *  需要先启动一个集群，保持一个会话，在这个会话中通过客户端提交作业。集群启动时所有的资源就都已经确定，所以所有提交的作业会竞争集群中的资源
 * 2.单作业模式
 * 会话模式因为资源共享会导致很多问题，所以为了更好地隔离资源，我们可以考虑为每个提交的作业启动一个集群 这个就是单作业模式(Per-Job)模式
 * 作业完成后，集群就会关闭，所有资源也会释放 这些特性使得单作业模式在生产环境更加稳定 所以是实际应用的首选模式 一般是用借助一些资源管理框架来启动集群
 * 3.应用模式(Application Mode)
 * 以上提到的两种模式都是在客户端上执行的。然后客户端提交给JobManager的。但是这种方式客户端需要占用大量网络带宽，去下载依赖和把二进制数据发送给JobManager
 * 加上很多情况下我们提交作业用的是同一个客户端，机会加重客户端所在节点的资源消耗
 * 解决方法就是我们不要客户端了 直接包应用提交到JobManager上运行。我们需要为每一个提交的应用单独启动一个JobManger,也就是创建一个集群。这个JobManager只为执行这个应用
 * 而存在，执行结束之后JobManager也就关闭了
 *
 */
public class WordCountStreamUnbounded {
    public static void main(String[] args) throws Exception {
        //MC TODO:创建执行环境
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(3);
        DataStreamSource<String> socketDS = env.socketTextStream("192.168.233.16", 7777);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = socketDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        String[] words = value.split(" ");
                        for (String word : words) {
                            collector.collect(Tuple2.of(word, 1));
                        }
                    }
                })
                .setParallelism(2)
                .returns(Types.TUPLE(Types.STRING,Types.INT))
                .keyBy(value -> value.f0)
                .sum(1);
        //MC TODO:输出
        sum.print();
        //MC TODO:执行
        env.execute();
    }
}
