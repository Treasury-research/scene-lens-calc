package com.knn3.bd.lens.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.math.BigInteger;

/**
 * @Author zhouyong
 * @File LensPublication
 * @Time 2022/12/20 11:14
 * @Description {
 * "type": "polygon_lens_publication",
 * "timestamp": "1670394828",
 * "data": {
 * "profileId": "104826",
 * "pubId": "2",
 * "contentURI": "https://arweave.net/H7bDEtAC6fIJilf7W6DS2PVUk7q9P8JOttB1XjppD2U",
 * "collectModule": "0x1292e6df9a4697daafddbd61d5a7545a634af33d",
 * "collectModuleReturnData": "0x00000000000000000000000000000000000000000000000000005af3107a40000000000000000000000000000d500b1d8e8ef31e21c99d1db9a6444d3adf1270000000000000000000000000a7324ea37c1e954513ce826d147a3b6da4bb9cb300000000000000000000000000000000000000000000000000000000000000c80000000000000000000000000000000000000000000000000000000000000000",
 * "referenceModule": "0x0000000000000000000000000000000000000000",
 * "referenceModuleReturnData": null,
 * "timestamp": "1670394828",
 * "transactionHash": "0x981d072181860f87749b05e5b5d1a6bba9ab86f998090d6c411e856ade5ad3e2",
 * "blockNumber": 36526709,
 * "type": "Post",
 * "id": 2
 * }
 * }
 */
@Setter
@Getter
@ToString
public class LensPublication {
    private String profileId;
    private String pubId;
    private String profileIdPointed;
    private String pubIdPointed;
    private String referenceModuleData;
    private String contentURI;
    private String collectModule;
    private String collectModuleReturnData;
    private String referenceModule;
    private String referenceModuleReturnData;
    private String timestamp;
    private String transactionHash;
    private Long blockNumber;
    private String type;
    private Integer id;

    // 需要解析的字段
    private String currency;
    private String recipient;
    private String amount;
    private BigInteger referralFee;
}
