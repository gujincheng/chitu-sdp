package com.chitu.bigdata.sdp.service;

import com.chitu.bigdata.sdp.api.bo.DiProjectBo;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@FeignClient(name= "bdp-stg")
public interface BdpService {
    @RequestMapping(value = "/auth/user/info",method= RequestMethod.GET)
    String getList(@RequestHeader MultiValueMap<String, String> headers);

    @RequestMapping(value = "/schedule/projects/diQuery",method= RequestMethod.POST)
    String getProjects(@RequestHeader MultiValueMap<String, String> headers,@RequestBody DiProjectBo diProjectBo);
}
