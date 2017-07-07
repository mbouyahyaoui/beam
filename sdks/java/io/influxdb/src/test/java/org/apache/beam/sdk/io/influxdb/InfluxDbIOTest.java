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
package org.apache.beam.sdk.io.influxdb;

import java.io.IOException;
import java.io.Serializable;
import java.net.ServerSocket;
import java.util.ArrayList;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

/**
 * Test on the MongoDbIO.
 */
public class InfluxDbIOTest implements Serializable {

  private static final String DATABASE = "beam";
  private static int port;

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  /**
   * Looking for an available network port.
   */
  @BeforeClass
  public static void availablePort() throws Exception {
    try (ServerSocket serverSocket = new ServerSocket(0)) {
      port = serverSocket.getLocalPort();
    }
  }

  @Test
  public void testWrite() throws Exception {
    ArrayList<String> data = new ArrayList<String>();
    for (int i = 0; i < 5; i++) {
      data.add(String.format("{\"scientist\":\"Test %s\"}", i));
    }
    PCollection<String> str = pipeline.apply(Create.of(data));
    str.apply(InfluxDbIO.write().withUri("http://localhost:8086").withDatabase(DATABASE));
    pipeline.run();
  }

  @Test
  public void testRead() throws Exception {
    PCollection<String> output = pipeline.apply(
        InfluxDbIO.<String>read()
            .withParser(new InfluxDbIO.Parser<String>() {
              @Override
              public String parse(String input) throws IOException {
                return input;
              }
            })
            .withUri("http://localhost:8086")
            .withDatabase("beam")
            .withCoder(StringUtf8Coder.of()));
    pipeline.run();
  }

}