package com.knn3.bd.lens.func;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.knn3.bd.lens.cons.Cons;
import com.knn3.bd.lens.model.DataWrapper;
import com.knn3.bd.rt.connector.kafka.SourceModel;
import com.knn3.bd.rt.utils.Json;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @Author zhouyong
 * @File LensMapFunction
 * @Time 2022/12/20 11:58
 * @Description 工程描述
 */
@Slf4j
public class LensMapFunction implements FlatMapFunction<SourceModel, Tuple2<String, DataWrapper>> {
    @Override
    public void flatMap(SourceModel sourceModel, Collector<Tuple2<String, DataWrapper>> out) throws Exception {
        try {
            DataWrapper wrapper = Json.MAPPER.readValue(sourceModel.getValue(), new TypeReference<DataWrapper>() {
            });
            wrapper.format();
            ObjectNode data = wrapper.getData();
            // 非收费数据
            if (data == null) return;

            if (Cons.PUBLICATION.equalsIgnoreCase(wrapper.getType())) {
                String profileId = data.get("profileId").asText();
                String pubId = data.get("pubId").asText();
                out.collect(Tuple2.of(String.join(Cons.SEP, wrapper.getType(), profileId, pubId), wrapper));
                return;
            }

            String transactionHash = data.get("transactionHash").asText();
            String logIndex = data.get("logIndex").asText();
            out.collect(Tuple2.of(String.join(Cons.SEP, wrapper.getType(), transactionHash, logIndex), wrapper));
        } catch (Exception e) {
            log.error("LensMapFunction,data={}", sourceModel);
            log.error("LensMapFunction", e);
        }
    }
}
