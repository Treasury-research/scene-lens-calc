package com.knn3.bd.lens;

import com.knn3.bd.lens.cons.Cons;
import com.knn3.bd.lens.func.LensDetailJoinFunction;
import com.knn3.bd.lens.func.LensDetailUnionFunction;
import com.knn3.bd.lens.func.LensDupFunction;
import com.knn3.bd.lens.func.LensMapFunction;
import com.knn3.bd.lens.model.DataWrapper;
import com.knn3.bd.lens.model.LensBroadModel;
import com.knn3.bd.lens.model.LensCollect;
import com.knn3.bd.lens.model.LensPublication;
import com.knn3.bd.lens.source.BroadHistorySource;
import com.knn3.bd.rt.Job;
import com.knn3.bd.rt.connector.kafka.KafkaSchema;
import com.knn3.bd.rt.connector.kafka.SourceModel;
import com.knn3.bd.rt.model.EnvConf;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

import java.util.List;
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
        String lensTopic = paramMap.get(EnvConf.KAFKA_TOPIC_POLYGON_LENS);
        Long startTime = Optional.ofNullable(paramMap.get(EnvConf.START_TIME)).map(Long::parseLong).orElse(null);

        int sourceP = Job.limit(p, 3);

        /**
         * kafka数据源  -1:earliest  0:latest  其他:指定时间戳
         */
        KafkaSource<SourceModel> lensSource = KafkaSource.<SourceModel>builder()
                .setBootstrapServers(sourceBrokers)
                .setTopics(lensTopic)
                .setClientIdPrefix(lensTopic)
                .setGroupId(jobName)
                .setStartingOffsets(startTime == -1L ? OffsetsInitializer.earliest() : (startTime == 0L ? OffsetsInitializer.latest() : OffsetsInitializer.timestamp(startTime)))
                .setDeserializer(new KafkaSchema())
                .build();

        // polygon_lens_currency "UQ_f7ee19bf8d9e7868265fe31b2ce" UNIQUE CONSTRAINT, btree ("transactionHash", "logIndex")
        // polygon_lens_treasury "UQ_7cc84951381807fceabf254d57c" UNIQUE CONSTRAINT, btree ("transactionHash", "logIndex")
        // polygon_lens_treasury_fee "UQ_c51d3ddb9f9c124c186db75a09b" UNIQUE CONSTRAINT, btree ("transactionHash", "logIndex")
        // polygon_lens_collect "UQ_bd7051c1920ef3b34a5123ed5e3" UNIQUE CONSTRAINT, btree ("transactionHash", "logIndex")
        // polygon_lens_publication "UQ_2495eaf75f154e2a9f9908d7469" UNIQUE CONSTRAINT, btree ("profileId", "pubId")
        OutputTag<LensPublication> pubTag = new OutputTag<>("polygon_lens_publication");
        OutputTag<LensCollect> collectTag = new OutputTag<>("polygon_lens_collect");

        MapStateDescriptor<String, List<LensBroadModel>> broadcastDescriptor = new MapStateDescriptor<>("BroadcastDescriptor", TypeInformation.of(String.class), TypeInformation.of(new TypeHint<List<LensBroadModel>>() {
        }));


        SingleOutputStreamOperator<DataWrapper> dupDs = env.fromSource(lensSource, WatermarkStrategy.noWatermarks(), "LensSource").name("LensSource").uid("LensSource").setParallelism(sourceP)
                .flatMap(new LensMapFunction()).name("LensMap").uid("LensMap")
                .keyBy(x -> x.f0)
                .process(new LensDupFunction(pubTag, collectTag)).name("LensDup").uid("LensDup");
        dupDs.getSideOutput(collectTag).keyBy((KeySelector<LensCollect, String>) value -> String.join(Cons.SEP, value.getRootProfileId(), value.getRootPubId()))
                .connect(dupDs.getSideOutput(pubTag).keyBy((KeySelector<LensPublication, String>) value -> String.join(Cons.SEP, value.getProfileId(), value.getPubId())))
                .process(new LensDetailJoinFunction()).name("Join").uid("Join")
                .connect(
                        env.addSource(new BroadHistorySource()).name("BroadHistory").uid("BroadHistory").setParallelism(1)
                                .union(dupDs)
                                .broadcast(broadcastDescriptor)
                )
                .process(new LensDetailUnionFunction(broadcastDescriptor)).name("BroadUnion").uid("BroadUnion")
                .print();


        env.execute(jobName);
    }
}
