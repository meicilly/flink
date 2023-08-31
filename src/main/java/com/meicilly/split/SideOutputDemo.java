package com.meicilly.split;

import com.meicilly.bean.WaterSensor;
import com.meicilly.functions.WaterSensorMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SideOutputDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("hadoop102", 7777)
                .map(new WaterSensorMapFunction());
        /**
         * TODO 使用侧输出流 实现分流
         * TODO 总结步骤
         *  1.使用process算子
         *  2.定义OutputTag对象
         *  3.调用ctx.output
         *  4.通过主流 获取 测流
         */

        /**
         * 创建OutputTag对象
         * 第一个参数： 标签名
         * 第二个参数： 放入侧输出流中的 数据的 类型，Typeinformation
         */
        OutputTag<WaterSensor> s1Tag = new OutputTag<>("s1", Types.POJO(WaterSensor.class));
        OutputTag<WaterSensor> s2Tag = new OutputTag<>("s2", Types.POJO(WaterSensor.class));

        SingleOutputStreamOperator<WaterSensor> process = sensorDS
                .process(
                        new ProcessFunction<WaterSensor, WaterSensor>() {
                            @Override
                            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                                String id = value.getId();
                                if ("s1".equals(id)) {
                                    // 如果是 s1，放到侧输出流s1中
                                    /**
                                     * 上下文ctx 调用ouput，将数据放入侧输出流
                                     * 第一个参数： Tag对象
                                     * 第二个参数： 放入侧输出流中的 数据
                                     */
                                    ctx.output(s1Tag, value);
                                } else if ("s2".equals(id)) {
                                    // 如果是 s2，放到侧输出流s2中

                                    ctx.output(s2Tag, value);
                                } else {
                                    // 非s1、s2的数据，放到主流中
                                    out.collect(value);
                                }

                            }
                        }
                );

        // 从主流中，根据标签 获取 侧输出流
        SideOutputDataStream<WaterSensor> s1 = process.getSideOutput(s1Tag);
        SideOutputDataStream<WaterSensor> s2 = process.getSideOutput(s2Tag);


        // 打印主流
        process.print("主流-非s1、s2");

        //打印 侧输出流
        s1.printToErr("s1");
        s2.printToErr("s2");

        env.execute();
    }
}
