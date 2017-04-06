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

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.beam.sdk.values.KV;

/**
 * An interface for real or fake implementations of Redis.
 */
interface RedisService extends Serializable {

  /**
   * The interface of a class that can read from Redis.
   */
  interface Reader {

    /**
     * Init the reader, including network connectionConfiguration and so.
     */
    boolean start() throws IOException;

    /**
     * Attempts to read the next element, and returns true if an element has been read.
     */
    boolean advance() throws IOException;

    /**
     * Closes the reader.
     */
    void close();

    /**
     * Returns the last row read by a successful start() or advance(), or throws if there is no
     * current row because the last such call was unsuccessful.
     */
    KV<String, String> getCurrent() throws NoSuchElementException;

  }

  /**
   * Returns a {@link Reader} that will read from the specified source.
   */
  Reader createReader(String keyPattern, RedisNode node) throws IOException;

  /**
   * Return an estimation of the size that could be read.
   */
  long getEstimatedSizeBytes();

  /**
   * Return true if the Redis instance support cluster, false, else.
   */
  boolean isClusterEnabled();

  /**
   * Return the list of Redis cluster nodes.
   */
  List<RedisNode> getClusterNodes();

  /**
   * Return the slot for a given key or pattern.
   */
  int getKeySlot(String key);

  /**
   * Describe a Redis node including the slot.
   */
  class RedisNode {

    public String host;
    public int port;
    public int timeout;
    public int startSlot;
    public int endSlot;
    public int index;
    public long size;

  }

}