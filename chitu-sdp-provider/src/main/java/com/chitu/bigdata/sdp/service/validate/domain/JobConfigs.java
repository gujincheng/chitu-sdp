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

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;


@Getter
@Setter
public class JobConfigs {

    private boolean useResult = true;
    private boolean fragment = true;
    private boolean useSession = false;
    private boolean useRemote = false;
    private String savePointPath = "";
    private String jobName = "草稿";
    private Integer clusterId = 0;
    private Integer checkPoint = 0;
    private Integer maxRowNum = 100;
    private Integer parallelism = 1;
    private String statement;
    private String type;
    private String session;
    private Integer taskId;
    private Integer clusterConfigurationId;
    private Integer savePointStrategy;
    private String configJson;
    private boolean useStatementSet;
    private static final ObjectMapper mapper = new ObjectMapper();

    private String address;
    private boolean useSqlFragment = false;
    private Integer checkpoint = 0;
    private boolean useRestAPI;

    private Map<String,String> config;

    public JobConfigs() {
    }

    public ExecutorSetting getExecutorSetting(){
        return new ExecutorSetting(checkpoint = 0,parallelism,useSqlFragment = false,savePointPath,jobName);
    }


}
