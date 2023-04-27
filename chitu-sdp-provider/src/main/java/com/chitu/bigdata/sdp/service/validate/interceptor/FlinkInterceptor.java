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
package com.chitu.bigdata.sdp.service.validate.interceptor;


import com.chitu.bigdata.sdp.service.validate.Operation;
import com.chitu.bigdata.sdp.service.validate.Operations;
import com.chitu.bigdata.sdp.service.validate.custom.CustomTableEnvironmentImpl;
import com.chitu.bigdata.sdp.service.validate.executor.Executor;
import com.chitu.bigdata.sdp.service.validate.function.FunctionManager;
import com.chitu.bigdata.sdp.service.validate.function.UDFunction;
import com.chitu.bigdata.sdp.service.validate.util.Asserts;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.functions.TableFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class FlinkInterceptor {

    public static String pretreatStatement(Executor executor, String statement) {
//        statement = SqlUtil.removeNote(statement);
//        statement = SqlUtil.clearNotes(statement);
        if(executor.isUseSqlFragment()) {
            statement = executor.getSqlManager().parseVariable(statement);
        }
        initFunctions(executor.getCustomTableEnvironmentImpl(), statement);
//        return statement.trim();
        return statement;
    }

    public static boolean build(Executor executor, String statement) {
        Operation operation = Operations.buildOperation(statement);
        if (Asserts.isNotNull(operation)) {
            operation.build(executor.getCustomTableEnvironmentImpl());
            return operation.noExecute();
        }
        return false;
    }

    private static void initFunctions(CustomTableEnvironmentImpl stEnvironment, String statement) {
        Map<String, UDFunction> usedFunctions = FunctionManager.getUsedFunctions(statement);
        String[] udfs = stEnvironment.listUserDefinedFunctions();
        List<String> udflist = Arrays.asList(udfs);
        for (Map.Entry<String, UDFunction> entry : usedFunctions.entrySet()) {
            if (!udflist.contains(entry.getKey())) {
                if (entry.getValue().getType() == UDFunction.UDFunctionType.Scalar) {
                    stEnvironment.registerFunction(entry.getKey(),
                            (ScalarFunction) entry.getValue().getFunction());
                } else if (entry.getValue().getType() == UDFunction.UDFunctionType.Table) {
                    stEnvironment.registerFunction(entry.getKey(),
                            (TableFunction) entry.getValue().getFunction());
                } else if (entry.getValue().getType() == UDFunction.UDFunctionType.Aggregate) {
                    stEnvironment.registerFunction(entry.getKey(),
                            (AggregateFunction) entry.getValue().getFunction());
                } else if (entry.getValue().getType() == UDFunction.UDFunctionType.TableAggregate) {
                    stEnvironment.registerFunction(entry.getKey(),
                            (TableAggregateFunction) entry.getValue().getFunction());
                }
            }
        }
    }

}
