package com.jd.realTimeDashboard;


import lombok.*;
import java.io.Serializable;

/**
 * 商品信息维表
 * {
 *  "merchandiseId": 187699,
 *  "merchandiseName": "iphone",
 *  "merchandiseType": "x"
 *  }
 */


@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class MerchandiseInfo implements Serializable {
    private static final long serialVersionUID = -6456352584841414358L;

    private long merchandiseId;
    private String merchandiseName;
    private String merchandiseType;

}
