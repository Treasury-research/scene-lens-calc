package com.knn3.bd.lens.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @Author zhouyong
 * @File LensCollect
 * @Time 2022/12/20 11:18
 * @Description {
 * "type": "polygon_lens_collect",
 * "timestamp": "1669811638",
 * "data": {
 * "collector": "0x77582a98132c2be11a6c0f85ca6361555a030a68",
 * "profileId": "100827",
 * "pubId": "140",
 * "rootProfileId": "100827",
 * "rootPubId": "140",
 * "collectModuleData": null,
 * "timestamp": "1669811638",
 * "transactionHash": "0xf40f2b4d4fd6b126ab728af72e8747698427242d9459acef59e290efe353ed62",
 * "transactionIndex": 25,
 * "blockNumber": 36253074,
 * "logIndex": 66,
 * "id": 39
 * }
 * }
 */
@Setter
@Getter
@ToString
public class LensCollect {
    private String collector;
    private String profileId;
    private String pubId;
    private String rootProfileId;
    private String rootPubId;
    private String collectModuleData;
    private String timestamp;
    private String transactionHash;
    private Integer transactionIndex;
    private Long blockNumber;
    private Integer logIndex;
    private Integer id;
    private String referral;
}
