package com.knn3.bd.lens;

import com.knn3.bd.rt.Job;
import com.knn3.bd.rt.connector.kafka.KafkaSchema;
import com.knn3.bd.rt.connector.kafka.SourceModel;
import com.knn3.bd.rt.model.EnvConf;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;
import java.util.Optional;

/**
 * @Author zhouyong
 * @File SceneLensCalcJob
 * @Time 2022/12/12 17:57
 * @Description 工程描述
 */
public class SceneLensCalcJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = Job.getEnv(args);
        int p = env.getParallelism();
        String jobName = SceneLensCalcJob.class.getSimpleName();
        Map<String, String> paramMap = env.getConfig().getGlobalJobParameters().toMap();
        String sourceBrokers = paramMap.get(EnvConf.KAFKA_BROKERS);
        // eth
        String eth20And721Topic = paramMap.get(EnvConf.KAFKA_TOPIC_TOKEN_TRANSFER);
        String eth1155Topic = paramMap.get(EnvConf.KAFKA_TOPIC_TOKEN_TRANSFER_1155);
        Long startTime = Optional.ofNullable(paramMap.get(EnvConf.START_TIME)).map(Long::parseLong).orElse(null);

        int sourceP = Job.limit(p, 3);

        /**
         * kafka数据源  -1:earliest  0:latest  其他:指定时间戳
         */
        KafkaSource<SourceModel> eth20And721Source = KafkaSource.<SourceModel>builder()
                .setBootstrapServers(sourceBrokers)
                .setTopics(eth20And721Topic)
                .setClientIdPrefix(eth20And721Topic)
                .setGroupId(jobName)
                .setStartingOffsets(startTime == -1L ? OffsetsInitializer.earliest() : (startTime == 0L ? OffsetsInitializer.latest() : OffsetsInitializer.timestamp(startTime)))
                .setDeserializer(new KafkaSchema())
                .build();

        env.fromSource(eth20And721Source, WatermarkStrategy.noWatermarks(), "Eth20And721").name("Eth20And721Source").uid("Eth20And721Source").setParallelism(sourceP);

        env.execute(jobName);
    }
}
