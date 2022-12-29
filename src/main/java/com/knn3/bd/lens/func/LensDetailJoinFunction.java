package com.knn3.bd.lens.func;

import com.google.common.collect.Iterables;
import com.knn3.bd.lens.model.LensCollect;
import com.knn3.bd.lens.model.LensDetail;
import com.knn3.bd.lens.model.LensPublication;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author zhouyong
 * @File LensDetailJoinFunction
 * @Time 2022/12/20 12:50
 * @Description polygon_lens_collect a  polygon_lens_publication b
 * a.root_pro_id=b.pro_id and a.root_pub_id=b.pub_id
 */
public class LensDetailJoinFunction extends KeyedCoProcessFunction<String, LensCollect, LensPublication, LensDetail> {
    private ValueState<LensPublication> pubState;
    private ListState<LensDetail> detailListState;

    private static void of(LensDetail detail, LensPublication publication) {
        detail.setPubType(publication.getType());
        detail.setAmountStr(publication.getAmount());
        detail.setCurrency(publication.getCurrency());
        detail.setReferralFee(publication.getReferralFee());

        Integer feeType;
        switch (publication.getCollectModule()) {
            case "0x1292e6df9a4697daafddbd61d5a7545a634af33d":
                feeType = 1;
                break;
            case "0xef13efa565fb29cd55ecf3de2beb6c69bd988212":
                feeType = 2;
                break;
            case "0xbf4e6c28d7f37c867ce62cf6ccb9efa4c7676f7f":
                feeType = 3;
                break;
            default:
                feeType = 4;
        }
        detail.setFeeType(feeType);

        detail.setOriginAddr(publication.getRecipient());
        // 原创无转载地址
        if (0 == detail.getRecipientType()) detail.setMirAddr(null);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<LensPublication> publicationValueStateDescriptor = new ValueStateDescriptor<>("pubState", LensPublication.class);
        ListStateDescriptor<LensDetail> detailListStateDescriptor = new ListStateDescriptor<>("detailListState", LensDetail.class);
        this.pubState = this.getRuntimeContext().getState(publicationValueStateDescriptor);
        this.detailListState = this.getRuntimeContext().getListState(detailListStateDescriptor);
    }

    @Override
    public void processElement1(LensCollect lensCollect, KeyedCoProcessFunction<String, LensCollect, LensPublication, LensDetail>.Context context, Collector<LensDetail> collector) throws Exception {
        LensDetail detail = new LensDetail();
        detail.setCollector(lensCollect.getCollector());
        detail.setProId(Integer.parseInt(lensCollect.getProfileId()));
        detail.setPubId(Integer.parseInt(lensCollect.getPubId()));
        detail.setRootProId(Integer.parseInt(lensCollect.getRootProfileId()));
        detail.setRootPubId(Integer.parseInt(lensCollect.getRootPubId()));
        detail.setRecipientType((detail.getRootProId().equals(detail.getProId()) && detail.getRootPubId().equals(detail.getPubId())) ? 0 : 1);
        detail.setBlkNum(lensCollect.getBlockNumber());
        detail.setHash(lensCollect.getTransactionHash());
        detail.setLogIdx(lensCollect.getLogIndex());
        detail.setIdx(lensCollect.getTransactionIndex());
        detail.setMirAddr(lensCollect.getReferral());
        detail.setTimestamp(Long.parseLong(lensCollect.getTimestamp()));
        LensPublication publication = this.pubState.value();
        if (publication == null) {
            this.detailListState.add(detail);
        } else {
            of(detail, publication);
            collector.collect(detail);
        }
    }

    @Override
    public void processElement2(LensPublication lensPublication, KeyedCoProcessFunction<String, LensCollect, LensPublication, LensDetail>.Context context, Collector<LensDetail> collector) throws Exception {
        this.pubState.update(lensPublication);
        if (Iterables.size(this.detailListState.get()) != 0) {
            for (LensDetail detail : this.detailListState.get()) {
                of(detail, lensPublication);
                collector.collect(detail);
            }
            this.detailListState.clear();
        }
    }
}
