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

public class Configuration {
        private String name;
        private String label;
        private SystemConfiguration.ValueType type;
        private Object defaultValue;
        private Object value;
        private String note;

        public Configuration(String name, String label, SystemConfiguration.ValueType type, Object defaultValue, String note) {
        this.name = name;
        this.label = label;
        this.type = type;
        this.defaultValue = defaultValue;
        this.value = defaultValue;
        this.note = note;
    }

        public String getName() {
        return name;
    }

        public void setName(String name) {
        this.name = name;
    }

        public String getLabel() {
        return label;
    }

        public void setLabel(String label) {
        this.label = label;
    }

        public SystemConfiguration.ValueType getType() {
        return type;
    }

        public void setType(SystemConfiguration.ValueType type) {
        this.type = type;
    }

        public Object getDefaultValue() {
        return defaultValue;
    }

        public void setDefaultValue(Object defaultValue) {
        this.defaultValue = defaultValue;
    }

        public Object getValue() {
        return value;
    }

        public void setValue(Object value) {
        this.value = value;
    }

        public String getNote() {
        return note;
    }

        public void setNote(String note) {
        this.note = note;
    }

}
