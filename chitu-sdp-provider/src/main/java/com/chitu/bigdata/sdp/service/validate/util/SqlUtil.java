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
package com.chitu.bigdata.sdp.service.validate.util;


public class SqlUtil {

    public static String[] getRemoveNotesStatements(String sql,String sqlSeparator){
        if(Asserts.isNullString(sql)){
            return new String[0];
        }
        return (removeNotes(sql) + "\n").split(sqlSeparator);
    }

    public static String[] getStatements(String sql,String sqlSeparator){
        if(Asserts.isNullString(sql)){
            return new String[0];
        }
        return sql.split(sqlSeparator);
    }

    public static String removeNote(String sql){
        if(Asserts.isNotNullString(sql)) {
            sql = sql.replaceAll("--([^'\r\n]{0,}('[^'\r\n]{0,}'){0,1}[^'\r\n]{0,}){0,}", "").trim();
        }
        return sql;
    }

    public static String clearNotes(String sql) {
        if(Asserts.isNotNullString(sql)) {
            sql = sql.replaceAll("/\\*(.|[\\r\\n])*?\\*/", "").trim();
        }
        return sql;
    }

    public static String removeNotes(String sql){
        sql = removeNote(sql);
        return clearNotes(sql);
    }

}
