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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.values.KV;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.util.JedisClusterCRC16;

/**
 * An implementation of the {@link RedisService} that actually communicates with a Redis server.
 */
public class RedisServiceImpl implements RedisService {

  private final RedisConnectionConfiguration connection;

  public RedisServiceImpl(RedisConnectionConfiguration connection) {
    this.connection = connection;
  }

  private class RedisReaderImpl extends BoundedSource.BoundedReader<KV<String, String>> {

    private final RedisIO.RedisSource source;

    private Jedis jedis;
    private ScanParams scanParams;
    private ScanResult<String> scanResult;
    private String nextCursor;
    private List<String> keys;
    private Pipeline pipeline;
    private List<KV<String, String>> batch;
    private Iterator<KV<String, String>> batchIterator;
    private KV<String, String> current;

    public RedisReaderImpl(RedisIO.RedisSource source) {
      this.source = source;
    }

    @Override
    public boolean start() {
      if (source.node != null && source.node.host != null) {
        jedis = new Jedis(source.node.host, source.node.port, connection.timeout());
        if (connection.auth() != null) {
          jedis.auth(connection.auth());
        }
      } else {
        jedis = connection.connect();
      }
      scanParams = new ScanParams();
      scanParams.match(source.keyPattern);

      scanResult = jedis.scan("0", scanParams);
      nextCursor = scanResult.getStringCursor();
      keys = scanResult.getResult();
      pipeline = jedis.pipelined();

      return advance();
    }

    @Override
    public boolean advance() {
      if (batchIterator != null && batchIterator.hasNext()) {
        current = batchIterator.next();
        return true;
      } else {
        if (keys != null) {
          batch = new LinkedList<>();
          for (String key : keys) {
            pipeline.get(key);
          }
          List<Object> values = pipeline.syncAndReturnAll();
          for (int i = 0; i < values.size(); i++) {
            batch.add(KV.of(keys.get(i), (String) values.get(i)));
          }
          batchIterator = batch.iterator();
          if (!nextCursor.equals("0")) {
            scanResult = jedis.scan(nextCursor, scanParams);
            nextCursor = scanResult.getStringCursor();
            keys = scanResult.getResult();
          } else {
            keys = null;
          }
          return advance();
        }
      }
      return false;
    }

    @Override
    public void close() {
      jedis.quit();
    }

    @Override
    public KV<String, String> getCurrent() throws NoSuchElementException {
      if (current == null) {
        throw new NoSuchElementException();
      }
      return current;
    }

    @Override
    public BoundedSource<KV<String, String>> getCurrentSource() {
      return source;
    }

  }

  @Override
  public RedisReaderImpl createReader(RedisIO.RedisSource source) {
    return new RedisReaderImpl(source);
  }

  /**
   * The estimate size bytes retrieves all keys from the database (no other way to add an accurate
   * estimation) and sum the value bytes length.
   */
  @Override
  public long getEstimatedSizeBytes(String keyPattern) {
    Jedis jedis = connection.connect();
    long size = 0;
    try {
      Set<String> keys = jedis.keys(keyPattern);
      for (String key : keys) {
        try {
          byte[] value = jedis.get(key).getBytes();
          size = size + value.length;
        } catch (Exception e) {
          e.printStackTrace();
          // TODO use hgetAll() ?
        }
      }
    } finally {
      jedis.quit();
    }
    return size;
  }

  @Override
  public boolean isClusterEnabled() {
    return connection.isClusterEnabled();
  }

  @Override
  public List<RedisNode> getClusterNodes() {
    return connection.getClusterNodes();
  }

  @Override
  public int getKeySlot(String key) {
    return JedisClusterCRC16.getSlot(key);
  }

}
