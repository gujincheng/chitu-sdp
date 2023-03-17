

package com.chitu.bigdata.sdp.test;

import com.alibaba.fastjson.JSON;
import com.chitu.bigdata.sdp.api.flink.ResourceValidateResp;
import com.chitu.bigdata.sdp.service.EngineService;
import com.chitu.cloud.web.test.BaseTest;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * TODO 类功能描述
 *
 * @author chenyun
 * @version 1.0
 * @date 2022/7/4 12:11
 */
public class ResourceValidate extends BaseTest {
    @Autowired
    private EngineService engineService;

    @Test
    public void testValidateResource(){

        ResourceValidateResp validateResp = engineService.checkResource(951L);

        System.out.println("=============="+JSON.toJSONString(validateResp));
    }


}
