package com.chitu.bigdata.sdp.service;

import org.springframework.web.bind.annotation.GetMapping;

/**
 * @author chenyun
 * @description: TODO
 * @date 2022/03/18 10:31
 */
//@FeignClient(name = "${feign.client.name}")
public interface ApiProjectService {

    @GetMapping(value = "/schedule/business/list")
    String queryBusinessLine();

}
