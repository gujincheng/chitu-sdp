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
package com.chitu.bigdata.sdp.flink.submit.service.impl

import com.chitu.bigdata.sdp.flink.submit.service.`trait`.FlinkSubmitTrait
import com.chitu.bigdata.sdp.flink.submit.service.{SubmitRequest, SubmitResponse}

import java.lang

object LocalSubmit extends FlinkSubmitTrait {
  override def doSubmit(submitInfo: SubmitRequest): SubmitResponse = {
    throw new UnsupportedOperationException("Unsupported local Submit ")
  }

  override def doStop(flinkHome: String, appId: String, jobStringId: String, savePoint: lang.Boolean, drain: lang.Boolean,cancelTimeout: Long): String = {
    throw new UnsupportedOperationException("Unsupported local Submit ")
  }

  override def doTriggerSavepoint(flinkHome: String, appId: String, jobStringId: String, savePoint: lang.Boolean, cancelTimeout: Long,savepointDirectory:String):   String =  {
    throw new UnsupportedOperationException("Unsupported local TriggerSavepoint ")
  }
}
