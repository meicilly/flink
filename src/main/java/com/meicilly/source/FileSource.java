package com.meicilly.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FileSource {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // TODO 从文件读： 新Source架构

        org.apache.flink.connector.file.src.FileSource<String> fileSource = org.apache.flink.connector.file.src.FileSource
                .forRecordStreamFormat(
                        new TextLineInputFormat(),
                        new Path("input/word.txt")
                )
                .build();

        env
                .fromSource(fileSource, WatermarkStrategy.noWatermarks(), "filesource")
                .print();


        env.execute();
    }
}
