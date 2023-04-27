/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.chitu.bigdata.sdp.service.validate.domain;

import com.chitu.bigdata.sdp.service.validate.enums.ActionType;
import com.chitu.bigdata.sdp.service.validate.enums.SavePointType;
import com.chitu.bigdata.sdp.service.validate.util.Asserts;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Getter
@Setter
public class FlinkConfig {
    private String jobName;
    private String jobId;
    private ActionType action;
    private SavePointType savePointType;
    private String savePoint;
    private List<ConfigPara> configParas;

    private static final ObjectMapper mapper = new ObjectMapper();

    public static final String DEFAULT_SAVEPOINT_PREFIX = "hdfs:///flink/savepoints/";
    public FlinkConfig() {
    }

    public FlinkConfig(List<ConfigPara> configParas) {
        this.configParas = configParas;
    }

    public FlinkConfig(String jobName, String jobId, ActionType action, SavePointType savePointType, String savePoint, List<ConfigPara> configParas) {
        this.jobName = jobName;
        this.jobId = jobId;
        this.action = action;
        this.savePointType = savePointType;
        this.savePoint = savePoint;
        this.configParas = configParas;
    }

    public static FlinkConfig build(Map<String, String> paras){
        List<ConfigPara> configParasList = new ArrayList<>();
        for (Map.Entry<String, String> entry : paras.entrySet()) {
            configParasList.add(new ConfigPara(entry.getKey(),entry.getValue()));
        }
        return new FlinkConfig(configParasList);
    }

    public static FlinkConfig build(String jobName, String jobId, String actionStr, String savePointTypeStr, String savePoint, String configParasStr){
        List<ConfigPara> configParasList = new ArrayList<>();
        JsonNode paras = null;
        if(Asserts.isNotNullString(configParasStr)) {
            try {
                paras = mapper.readTree(configParasStr);
            } catch (Exception e) {
                e.printStackTrace();
            }
            paras.forEach((JsonNode node) -> {
                        configParasList.add(new ConfigPara(node.get("key").asText(), node.get("value").asText()));
                    }
            );
        }
        return new FlinkConfig(jobName,jobId,ActionType.get(actionStr),SavePointType.get(savePointTypeStr),savePoint,configParasList);
    }

    public static FlinkConfig build(String jobId, String actionStr, String savePointTypeStr, String savePoint){
        return new FlinkConfig(null,jobId,ActionType.get(actionStr),SavePointType.get(savePointTypeStr),savePoint,null);
    }
}

