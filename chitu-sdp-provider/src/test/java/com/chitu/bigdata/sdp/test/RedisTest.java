package com.chitu.bigdata.sdp.test;

import com.chitu.bigdata.sdp.constant.RedisKeyConstant;
import com.chitu.cloud.web.test.BaseTest;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;

/**
 * @author sutao
 * @create 2022-05-10 12:00
 */
public class RedisTest extends BaseTest {

    @Autowired
    private RedisTemplate<String, String> redisTmplate;


    @Test
    public void testMap() {

        String key = RedisKeyConstant.MONITOR_LAST_DELAY.getKey();
        redisTmplate.opsForHash().put(key,"9999:t1","100");
        Object lastValue = redisTmplate.opsForHash().get(key,"9999:t1");

        Object lastValue1 = redisTmplate.opsForHash().get(key,"9999:t2");
        if(lastValue != null){
            System.out.println(lastValue);
        }

        System.out.println("100".equals(lastValue));

        Object lastValue2 = redisTmplate.opsForHash().get("monitor:lastdelayxxx","9999:t2");

        System.out.println("");



    }
}

