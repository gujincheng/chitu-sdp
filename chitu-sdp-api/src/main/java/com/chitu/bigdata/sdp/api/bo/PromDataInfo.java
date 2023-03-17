package com.chitu.bigdata.sdp.api.bo;

import lombok.Data;

import java.util.List;

/**
 * @author sutao
 * @create 2021-11-10 22:43
 */

@Data
public class PromDataInfo {

    /**
     * prometheus结果类型
     * vector--瞬时向量
     * matrix--区间向量
     * scalar--标量
     * string--字符串
     */
    private String resultType;

    /**
     * prometheus指标属性和值
     */
    private List<PromResultInfo> result;

}
