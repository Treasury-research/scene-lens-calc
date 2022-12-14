package com.knn3.bd.lens.func;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.knn3.bd.lens.cons.Cons;
import com.knn3.bd.lens.model.DataWrapper;
import com.knn3.bd.lens.model.LensBroadModel;
import com.knn3.bd.lens.model.LensDetail;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @Author zhouyong
 * @File LensDetailUnionFunction
 * @Time 2022/12/20 12:51
 * @Description 工程描述
 */
@Slf4j
public class LensDetailUnionFunction extends BroadcastProcessFunction<LensDetail, DataWrapper, LensDetail> {
    private final MapStateDescriptor<String, List<LensBroadModel>> broadcastDescriptor;

    public LensDetailUnionFunction(MapStateDescriptor<String, List<LensBroadModel>> broadcastDescriptor) {
        this.broadcastDescriptor = broadcastDescriptor;
    }

    private static LensBroadModel ofCurrency(ObjectNode node) {
        LensBroadModel model = new LensBroadModel();
        model.setCurrency(node.get("currency").asText());
        model.setTimestamp(Long.parseLong(node.get("timestamp").asText()));
        model.setDecimals(Integer.parseInt(node.get("decimals").asText()));
        return model;
    }

    private static LensBroadModel ofTreasury(ObjectNode node) {
        LensBroadModel model = new LensBroadModel();
        model.setNewTreasury(node.get("newTreasury").asText());
        model.setTimestamp(Long.parseLong(node.get("timestamp").asText()));
        model.setBlockNumber(Long.parseLong(node.get("blockNumber").asText()));
        model.setTransactionIndex(Integer.parseInt(node.get("transactionIndex").asText()));
        return model;
    }

    private static LensBroadModel ofTreasuryFee(ObjectNode node) {
        LensBroadModel model = new LensBroadModel();
        model.setNewTreasuryFee(Long.parseLong(node.get("newTreasuryFee").asText()));
        model.setTimestamp(Long.parseLong(node.get("timestamp").asText()));
        model.setBlockNumber(Long.parseLong(node.get("blockNumber").asText()));
        model.setTransactionIndex(Integer.parseInt(node.get("transactionIndex").asText()));
        return model;
    }

    @Override
    public void processElement(LensDetail detail, BroadcastProcessFunction<LensDetail, DataWrapper, LensDetail>.ReadOnlyContext context, Collector<LensDetail> collector) throws Exception {
        try {
            String currency = detail.getCurrency();
            Long blkNum = detail.getBlkNum();
            Integer idx = detail.getIdx();
            detail.setDecimals(
                    context.getBroadcastState(this.broadcastDescriptor).get(Cons.CURRENCY)
                            .stream()
                            .filter(x -> currency.equals(x.getCurrency()))
                            .map(LensBroadModel::getDecimals)
                            .collect(Collectors.toList()).get(0)
            );

            // 平台地址
            for (LensBroadModel model : context.getBroadcastState(this.broadcastDescriptor).get(Cons.TREASURY)) {
                Long blockNumber = model.getBlockNumber();
                Integer index = model.getTransactionIndex();
                if (blkNum > blockNumber || (blkNum.equals(blockNumber) && idx >= index)) {
                    detail.setPlatAddr(model.getNewTreasury());
                    break;
                }
            }

            // 平台费用
            for (LensBroadModel model : context.getBroadcastState(this.broadcastDescriptor).get(Cons.TREASURY_FEE)) {
                Long blockNumber = model.getBlockNumber();
                Integer index = model.getTransactionIndex();
                if (blkNum > blockNumber || (blkNum.equals(blockNumber) && idx >= index)) {
                    detail.setPlatRate(model.getNewTreasuryFee());
                    break;
                }
            }
            // 计算三种金额
            String amountStr = (detail.getAmountStr().length() - detail.getDecimals() <= 0 ? String.join("", IntStream.range(0, detail.getDecimals() + 1 - detail.getAmountStr().length()).mapToObj(x -> "0").collect(Collectors.toList())) : "") + detail.getAmountStr();
            double amount = Double.parseDouble(new StringBuilder().append(amountStr).insert(amountStr.length() - detail.getDecimals(), ".").toString());
            detail.setAmount(amount);
            detail.setPlatAmount(amount * detail.getPlatRate() / 10000);
            detail.setMirAmount(detail.getRecipientType() == 1 ? amount * detail.getReferralFee().longValue() / 10000 : 0);
            detail.setOriginAmount(amount - detail.getPlatAmount() - detail.getMirAmount());
            collector.collect(detail);
        } catch (Exception e) {
            log.error("LensDetailUnionFunction,processElement,data={}", detail);
            log.error("LensDetailUnionFunction,processElement", e);
        }
    }

    @Override
    public void processBroadcastElement(DataWrapper wrapper, BroadcastProcessFunction<LensDetail, DataWrapper, LensDetail>.Context context, Collector<LensDetail> collector) throws Exception {
        try {
            String type = wrapper.getType();
            ObjectNode node = wrapper.getData();
            LensBroadModel model;
            switch (type) {
                case Cons.CURRENCY:
                    model = ofCurrency(node);
                    break;
                case Cons.TREASURY:
                    model = ofTreasury(node);
                    break;
                default:
                    model = ofTreasuryFee(node);
            }
            List<LensBroadModel> modelList = Optional.ofNullable(context.getBroadcastState(this.broadcastDescriptor).get(type))
                    .orElseGet(ArrayList::new);
            modelList.add(model);
            modelList.sort(Comparator.comparing(x -> x.getTimestamp() * -1));
            context.getBroadcastState(this.broadcastDescriptor).put(type, modelList);
        } catch (Exception e) {
            log.error("LensDetailUnionFunction,data={}", wrapper);
            log.error("LensDetailUnionFunction", e);
        }
    }
}
