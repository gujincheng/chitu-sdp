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
package com.chitu.bigdata.sdp.service.validate;

import com.chitu.bigdata.sdp.service.validate.custom.CustomTableEnvironmentImpl;
import com.chitu.bigdata.sdp.service.validate.parser.SingleSqlParserFactory;
import com.chitu.bigdata.sdp.service.validate.util.Asserts;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SetOperation extends AbstractOperation implements Operation {

    private String KEY_WORD = "SET";

    public SetOperation() {
    }

    public SetOperation(String statement) {
        super(statement);
    }

    @Override
    public String getHandle() {
        return KEY_WORD;
    }

    @Override
    public Operation create(String statement) {
        return new SetOperation(statement);
    }

    @Override
    public void build(CustomTableEnvironmentImpl stEnvironment) {
        Map<String,List<String>> map = SingleSqlParserFactory.generateParser(statement);
        if(Asserts.isNotNullMap(map)&&map.size()==2) {
            Map<String, String> confMap = new HashMap<>();
            confMap.put(StringUtils.join(map.get("SET"), "."), StringUtils.join(map.get("="), ","));
            stEnvironment.getConfig().addConfiguration(Configuration.fromMap(confMap));
        }
    }
}
