package com.knn3.bd.lens.model;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.knn3.bd.lens.cons.Cons;
import com.knn3.bd.rt.utils.Json;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @Author zhouyong
 * @File DataWrapper
 * @Time 2022/12/19 18:17
 * @Description 工程描述
 */
@Setter
@Getter
@ToString
public class DataWrapper {
    private String type;
    @JsonDeserialize(using = StringToLong.class)
    private Long timestamp;
    @JsonDeserialize(using = StringToJson.class)
    private ObjectNode data;

    public void format() {
        switch (this.type) {
            case Cons.CURRENCY:
                this.data.put("currency", this.data.remove("currency").asText().toLowerCase(Locale.ROOT));
                return;
            case Cons.PUBLICATION:
                String collectModule = this.data.get("collectModule").asText();
                // 不收费的数据,过滤掉
                if (!Cons.FEE_VALUE_MAP.containsKey(collectModule)) this.data = null;
                else {
                    // 小写
                    this.data.put("type", this.data.remove("type").asText().toLowerCase(Locale.ROOT));
                    String cmr = this.data.remove("collectModuleReturnData").asText().toLowerCase(Locale.ROOT).replaceAll("\\s", "").substring(2);
                    this.data.setAll(this.parCMRData(cmr, collectModule));
                }
                return;
        }
    }

    /**
     * 解析collectModuleReturnData,解析完成后删除collectModuleReturnData字段,添加解析后的字段
     *
     * @param cmr
     * @param collectModule
     * @return
     */
    private ObjectNode parCMRData(String cmr, String collectModule) {
        List<String> columns = Cons.FEE_MAP.get(Cons.FEE_VALUE_MAP.get(collectModule));
        int part = cmr.length() / columns.size();
        Map<String, String> columnMap = IntStream.range(0, columns.size()).mapToObj(x -> String.join(",", columns.get(x), cmr.substring(part * x, part * (x + 1)))).collect(Collectors.toMap(x -> x.split(",")[0], x -> x.split(",")[1]));
        ObjectNode node = Json.MAPPER.createObjectNode();
        // // 取后面的40位 = > 0x + 40位
        node.put("currency", String.format("0x%s", columnMap.get("currency").substring(columnMap.get("currency").length() - 40)));
        node.put("recipient", String.format("0x%s", columnMap.get("recipient").substring(columnMap.get("recipient").length() - 40)));
        // // 16进制转10进制
        node.put("amount", new BigInteger(columnMap.get("amount"), 16).toString());
        node.put("referralFee", new BigInteger(columnMap.get("referralFee"), 16));
        return node;
    }

    private static class StringToLong extends JsonDeserializer<Long> {
        @Override
        public Long deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
            return Long.parseLong(jsonParser.getText()) * 1000;
        }
    }

    private static class StringToJson extends JsonDeserializer<ObjectNode> {
        @Override
        public ObjectNode deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
            return jsonParser.getCodec().readTree(jsonParser);
        }
    }

}
