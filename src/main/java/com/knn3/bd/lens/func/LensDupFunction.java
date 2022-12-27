package com.knn3.bd.lens.func;

import com.fasterxml.jackson.core.type.TypeReference;
import com.knn3.bd.lens.cons.Cons;
import com.knn3.bd.lens.model.DataWrapper;
import com.knn3.bd.lens.model.LensCollect;
import com.knn3.bd.lens.model.LensPublication;
import com.knn3.bd.rt.utils.Json;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Author zhouyong
 * @File LensDupDunction
 * @Time 2022/12/20 12:27
 * @Description 工程描述
 */
public class LensDupFunction extends KeyedProcessFunction<String, Tuple2<String, DataWrapper>, DataWrapper> {
    private final OutputTag<LensPublication> pubTag;
    private final OutputTag<LensCollect> collectTag;
    private MapState<String, Integer> hashIdxMapState;

    public LensDupFunction(OutputTag<LensPublication> pubTag, OutputTag<LensCollect> collectTag) {
        this.pubTag = pubTag;
        this.collectTag = collectTag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        StateTtlConfig ttl = StateTtlConfig
                .newBuilder(Time.days(7))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupInRocksdbCompactFilter(1000)
                .build();
        MapStateDescriptor<String, Integer> descriptor = new MapStateDescriptor<>("hash-idx", String.class, Integer.class);
        descriptor.enableTimeToLive(ttl);
        this.hashIdxMapState = this.getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public void processElement(Tuple2<String, DataWrapper> t, KeyedProcessFunction<String, Tuple2<String, DataWrapper>, DataWrapper>.Context context, Collector<DataWrapper> collector) throws Exception {
        // 如果hashIdx相同,代表重复了,直接返回
        if (this.hashIdxMapState.get(t.f0) != null) return;
        this.hashIdxMapState.put(t.f0, 0);
        String type = t.f1.getType();
        if (type.equalsIgnoreCase(Cons.PUBLICATION))
            context.output(this.pubTag, Json.MAPPER.readValue(t.f1.getData().toString(), new TypeReference<LensPublication>() {
            }));
        else if (type.equalsIgnoreCase(Cons.COLLECT))
            context.output(this.collectTag, Json.MAPPER.readValue(t.f1.getData().toString(), new TypeReference<LensCollect>() {
            }));
        else collector.collect(t.f1);
    }
}
