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
package org.apache.beam.sdk.io.redis;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.testing.TestPipelineOptions;

/**
 * These options can be used by a test connecting to a Redis instance to configure the connection.
 */
public interface RedisTestOptions extends TestPipelineOptions {

  @Description("Location of the Redis instance (host:port)")
  @Default.String("redis-location")
  String getRedisLocation();
  void setRedisLocation(String redisLocation);

  @Description("Name of the Redis PubSub channel")
  @Default.String("redis-channel")
  String getRedisPubSubChannel();
  void setRedisPubSubChannel(String redisChannel);

}
