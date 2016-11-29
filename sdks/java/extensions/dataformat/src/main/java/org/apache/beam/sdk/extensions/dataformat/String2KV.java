/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.extensions.dataformat;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

/**
 * Convert a String as KV.
 */
public class String2KV extends DoFn<String, KV<String, String>> {

  private String separator; // TODO use a Regex Pattern

  public String2KV() {
    this.separator = ":";
  }

  public String2KV(String separator) {
    this.separator = separator;
  }

  @ProcessElement
  public void processElement(ProcessContext context) {
    String input = context.element();
    String[] split = input.split(separator);
    if (split.length != 2) {
      throw new IllegalStateException("String2KV failed due to invalid split");
    }
    context.output(KV.of(split[0], split[1]));
  }

}
