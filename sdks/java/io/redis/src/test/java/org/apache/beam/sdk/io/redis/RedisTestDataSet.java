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

import redis.clients.jedis.Jedis;

/**
 * Util class to generate and manipulate test dataset.
 */
public class RedisTestDataSet {

  public static Jedis getJedis(RedisTestOptions options) {
    String[] redisLocation = options.getRedisLocation().split(":");
    if (redisLocation.length != 2) {
      throw new IllegalArgumentException("RedisLocation is malformed");
    }
    return new Jedis(redisLocation[0], Integer.parseInt(redisLocation[1]));
  }

  public static void createTestDataset(Jedis jedis) {
    jedis.connect();
    for (int i = 0; i < 1000; i++) {
      jedis.set("Key " + i, "Value " + i);
    }
    jedis.quit();
  }

  public static void deleteTestDataset(Jedis jedis) {
    jedis.connect();
    for (int i = 0; i < 1000; i++) {
      jedis.del("Key " + i);
    }
    jedis.quit();
  }

}
