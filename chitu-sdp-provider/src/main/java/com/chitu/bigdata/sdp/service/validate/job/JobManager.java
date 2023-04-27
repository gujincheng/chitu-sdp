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
package com.chitu.bigdata.sdp.service.validate.job;


import com.chitu.bigdata.sdp.service.validate.SystemConfiguration;
import com.chitu.bigdata.sdp.service.validate.constant.FlinkSQLConstant;
import com.chitu.bigdata.sdp.service.validate.custom.CustomTableEnvironmentImpl;
import com.chitu.bigdata.sdp.service.validate.domain.ExplainResult;
import com.chitu.bigdata.sdp.service.validate.domain.JobConfigs;
import com.chitu.bigdata.sdp.service.validate.executor.Executor;
import com.chitu.bigdata.sdp.service.validate.explainer.Explainer;

public class JobManager {

    private JobConfigs config;
    private Executor executor;
    private boolean useStatementSet;
    private String sqlSeparator = FlinkSQLConstant.SEPARATOR;

    public JobManager() {
    }

    public JobManager(JobConfigs config) {
        this.config = config;
    }

    public static JobManager build() {
        JobManager manager = new JobManager();
        manager.init();
        return manager;
    }

    public static JobManager build(JobConfigs config) {
        JobManager manager = new JobManager(config);
        manager.init();
        return manager;
    }

    private Executor createExecutor() {
        executor = Executor.buildLocalExecutor(config.getExecutorSetting());
        return executor;
    }

    private Executor createExecutorWithSession() {
        createExecutor();
        return executor;
    }

    public boolean init() {
        useStatementSet = config.isUseStatementSet();
        sqlSeparator = SystemConfiguration.getInstances().getSqlSeparator();
        createExecutorWithSession();
        return false;
    }


    public ExplainResult explainSql(String content) {
        Explainer explainer = Explainer.build(executor,useStatementSet,sqlSeparator);
        return explainer.explainSql(content);
    }

    public CustomTableEnvironmentImpl getEnv(){
        return executor.getCustomTableEnvironmentImpl();
    }

}
