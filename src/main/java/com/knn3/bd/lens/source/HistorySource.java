package com.knn3.bd.lens.source;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.knn3.bd.lens.cons.Cons;
import com.knn3.bd.lens.model.DataWrapper;
import com.knn3.bd.rt.connector.kafka.SourceModel;
import com.knn3.bd.rt.model.EnvConf;
import com.knn3.bd.rt.utils.JDBCUtils;
import com.knn3.bd.rt.utils.Json;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ArrayListHandler;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * @Author zhouyong
 * @File BroadHistorySource
 * @Time 2022/12/21 18:17
 * @Description 工程描述
 */
public class HistorySource extends RichSourceFunction<SourceModel> {
    @Override
    public void run(SourceContext<SourceModel> sourceContext) throws Exception {
        Map<String, String> confMap = this.getRuntimeContext().getExecutionConfig().getGlobalJobParameters().toMap();
        String url = confMap.get(EnvConf.PG_URL);
        String username = confMap.get(EnvConf.PG_USERNAME);
        String password = confMap.get(EnvConf.PG_PASSWORD);
        JDBCUtils.loadDriver(JDBCUtils.POSTGRES);
        QueryRunner qr = new QueryRunner(JDBCUtils.getDataSource(url, username, password));
        qr.query("select currency,decimals,timestamp from polygon_lens_currency", new ArrayListHandler())
                .stream().map(x -> {
                    try {
                        SourceModel sourceModel = new SourceModel();
                        DataWrapper wrapper = new DataWrapper();
                        ObjectNode node = Json.MAPPER.createObjectNode();
                        node.put("currency", x[0].toString());
                        node.put("decimals", x[1].toString());
                        node.put("timestamp", x[2].toString());
                        wrapper.setType(Cons.CURRENCY);
                        wrapper.setData(node);
                        sourceModel.setValue(Json.MAPPER.writeValueAsString(wrapper));
                        return sourceModel;
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }
                    return null;
                }).collect(Collectors.toList()).forEach(sourceContext::collect);

        qr.query("select \"blockNumber\",\"transactionIndex\",\"newTreasury\",timestamp as treasury from polygon_lens_treasury", new ArrayListHandler())
                .stream().map(x -> {
                    try {
                        SourceModel sourceModel = new SourceModel();
                        DataWrapper wrapper = new DataWrapper();
                        ObjectNode node = Json.MAPPER.createObjectNode();
                        node.put("blockNumber", Long.parseLong(x[0].toString()));
                        node.put("transactionIndex", Integer.parseInt(x[1].toString()));
                        node.put("newTreasury", x[2].toString());
                        node.put("timestamp", Long.parseLong(x[3].toString()));
                        wrapper.setType(Cons.TREASURY);
                        wrapper.setData(node);
                        sourceModel.setValue(Json.MAPPER.writeValueAsString(wrapper));
                        return sourceModel;
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }
                    return null;
                }).collect(Collectors.toList()).forEach(sourceContext::collect);

        qr.query("select \"blockNumber\",\"transactionIndex\",\"newTreasuryFee\",timestamp as treasury from polygon_lens_treasury_fee", new ArrayListHandler())
                .stream().map(x -> {
                    try {
                        SourceModel sourceModel = new SourceModel();
                        DataWrapper wrapper = new DataWrapper();
                        ObjectNode node = Json.MAPPER.createObjectNode();
                        node.put("blockNumber", Long.parseLong(x[0].toString()));
                        node.put("transactionIndex", Integer.parseInt(x[1].toString()));
                        node.put("newTreasuryFee", x[2].toString());
                        node.put("timestamp", Long.parseLong(x[3].toString()));
                        wrapper.setType(Cons.TREASURY_FEE);
                        wrapper.setData(node);
                        sourceModel.setValue(Json.MAPPER.writeValueAsString(wrapper));
                        return sourceModel;
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }
                    return null;
                }).collect(Collectors.toList()).forEach(sourceContext::collect);

        qr.query("select \"profileId\",\"pubId\",\"collectModuleReturnData\",type,\"collectModule\",\"transactionHash\",\"logIndex\" from polygon_lens_publication where \"collectModule\" in ('0x1292e6df9a4697daafddbd61d5a7545a634af33d','0xef13efa565fb29cd55ecf3de2beb6c69bd988212','0xbf4e6c28d7f37c867ce62cf6ccb9efa4c7676f7f','0x7b94f57652cc1e5631532904a4a038435694636b')", new ArrayListHandler())
                .stream().map(x -> {
                    try {
                        SourceModel sourceModel = new SourceModel();
                        DataWrapper wrapper = new DataWrapper();
                        ObjectNode node = Json.MAPPER.createObjectNode();
                        node.put("profileId", Long.parseLong(x[0].toString()));
                        node.put("pubId", Integer.parseInt(x[1].toString()));
                        node.put("collectModuleReturnData", x[2].toString());
                        node.put("type", x[3].toString());
                        node.put("collectModule", x[4].toString());
                        node.put("transactionHash", x[5].toString());
                        node.put("logIndex", x[6].toString());
                        wrapper.setType(Cons.PUBLICATION);
                        wrapper.setData(node);
                        sourceModel.setValue(Json.MAPPER.writeValueAsString(wrapper));
                        return sourceModel;
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }
                    return null;
                }).collect(Collectors.toList()).forEach(sourceContext::collect);
    }

    @Override
    public void cancel() {

    }
}
