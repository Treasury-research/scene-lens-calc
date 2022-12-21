package com.knn3.bd.lens.source;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.knn3.bd.lens.cons.Cons;
import com.knn3.bd.lens.model.DataWrapper;
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
public class BroadHistorySource extends RichSourceFunction<DataWrapper> {
    @Override
    public void run(SourceContext<DataWrapper> sourceContext) throws Exception {
        Map<String, String> confMap = this.getRuntimeContext().getExecutionConfig().getGlobalJobParameters().toMap();
        String url = confMap.get(EnvConf.PG_URL);
        String username = confMap.get(EnvConf.PG_USERNAME);
        String password = confMap.get(EnvConf.PG_PASSWORD);
        JDBCUtils.loadDriver(JDBCUtils.POSTGRES);
        QueryRunner qr = new QueryRunner(JDBCUtils.getDataSource(url, username, password));
        qr.query("select currency,decimals,timestamp from polygon_lens_currency", new ArrayListHandler())
                .stream().map(x -> {
                    DataWrapper wrapper = new DataWrapper();
                    ObjectNode node = Json.MAPPER.createObjectNode();
                    node.put("currency", x[0].toString());
                    node.put("decimals", x[1].toString());
                    node.put("timestamp", x[2].toString());
                    wrapper.setType(Cons.CURRENCY);
                    wrapper.setData(node);
                    return wrapper;
                }).collect(Collectors.toList()).forEach(sourceContext::collect);

        qr.query("select \"blockNumber\",\"transactionIndex\",\"newTreasury\",timestamp as treasury from polygon_lens_treasury", new ArrayListHandler())
                .stream().map(x -> {
                    DataWrapper wrapper = new DataWrapper();
                    ObjectNode node = Json.MAPPER.createObjectNode();
                    node.put("blockNumber", Long.parseLong(x[0].toString()));
                    node.put("transactionIndex", Integer.parseInt(x[1].toString()));
                    node.put("newTreasury", x[2].toString());
                    node.put("timestamp", Long.parseLong(x[3].toString()));
                    wrapper.setType(Cons.TREASURY);
                    wrapper.setData(node);
                    return wrapper;
                }).collect(Collectors.toList()).forEach(sourceContext::collect);

        qr.query("select \"blockNumber\",\"transactionIndex\",\"newTreasuryFee\",timestamp as treasury from polygon_lens_treasury_fee", new ArrayListHandler())
                .stream().map(x -> {
                    DataWrapper wrapper = new DataWrapper();
                    ObjectNode node = Json.MAPPER.createObjectNode();
                    node.put("blockNumber", Long.parseLong(x[0].toString()));
                    node.put("transactionIndex", Integer.parseInt(x[1].toString()));
                    node.put("newTreasuryFee", x[2].toString());
                    node.put("timestamp", Long.parseLong(x[3].toString()));
                    wrapper.setType(Cons.TREASURY);
                    wrapper.setData(node);
                    return wrapper;
                }).collect(Collectors.toList()).forEach(sourceContext::collect);
    }

    @Override
    public void cancel() {

    }
}
