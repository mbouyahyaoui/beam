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
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

/**
 * An implementation of the {@link RedisService} that actually communicates with a Redis server.
 */
public class RedisServiceImpl implements RedisService {

  private final RedisConnection connection;

  public RedisServiceImpl(RedisConnection connection) {
    this.connection = connection;
  }

  private class RedisWriterImpl implements Writer {

    private Jedis jedis;

    public RedisWriterImpl() {
    }

    @Override
    public void start() {
      jedis = connection.connect();
    }

    @Override
    public void write(KV<String, String> record, RedisIO.Write.Command command) {
      if (command == RedisIO.Write.Command.SET) {
        jedis.set(record.getKey(), record.getValue());
      }
      if (command == RedisIO.Write.Command.APPEND) {
        jedis.append(record.getKey(), record.getValue());
      }
      if (command == RedisIO.Write.Command.SETNX) {
        jedis.setnx(record.getKey(), record.getValue());
      }
      /*
      jedis.incrBy(element.getKey(), Long.parseLong(element.getValue()));
      jedis.decrBy(element.getKey(), Long.parseLong(element.getValue()));
      jedis.del(element.getKey());
      jedis.rename(element.getKey(), element.getValue());
      jedis.renamenx(element.getKey(), element.getValue());
      jedis.expire(element.getKey(), Integer.parseInt(element.getValue()));
      jedis.expireAt(element.getKey(), Long.parseLong(element.getValue()));
      jedis.pexpire(element.getKey(), Integer.parseInt(element.getValue()));
      jedis.pexpireAt(element.getKey(), Long.parseLong(element.getValue()));
      jedis.move(element.getKey(), Integer.parseInt(element.getValue()));
      jedis.incrByFloat(element.getKey(), Float.parseFloat(element.getValue()));
      jedis.hdel(element.getKey(), element.getValue());
      jedis.rpush(element.getKey(), element.getValue());
      jedis.lpush(element.getKey(), element.getValue());
      jedis.rpoplpush(element.getKey(), element.getValue());
      jedis.sadd(element.getKey(), element.getValue());
      jedis.srem(element.getKey(), element.getValue());
      */
    }

    @Override
    public void close() {
      jedis.quit();
    }

  }

  @Override
  public Writer createWriter() {
    return new RedisWriterImpl();
  }

  private class RedisReaderImpl implements Reader {

    private final String keyPattern;
    private Jedis jedis;
    private Iterator<String> keysIterator;
    private KV<String, String> current;

    public RedisReaderImpl(String keyPattern) {
      this.keyPattern = keyPattern;
    }

    @Override
    public boolean start() {
      jedis = connection.connect();
      Set<String> keys = jedis.keys(keyPattern);
      keysIterator = keys.iterator();
      return advance();
    }

    @Override
    public boolean advance() {
      if (keysIterator.hasNext()) {
        String key = keysIterator.next();
        String value = jedis.get(key);
        KV<String, String> kv = KV.of(key, value);
        current = kv;
        return true;
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

  }

  @Override
  public Reader createReader(String keyPattern) throws IOException {
    return new RedisReaderImpl(keyPattern);
  }

  /**
   * The estimate size bytes is based on sampling, computing average size of 10 random
   * key/value pairs. This sampling average size is used with the Redis dbSize to get an
   * estimation of the actual database size.
   *
   * @return The estimated size of the Redis database in bytes.
   */
  @Override
  public long getEstimatedSizeBytes() {
    Jedis jedis = connection.connect();
    // estimate the size of a key/value pair using sampling
    long samplingSize = 0;
    for (int i = 0; i < 10; i++) {
      String key = jedis.randomKey();
      if (key != null) {
        samplingSize = samplingSize + key.getBytes().length;
        String value = jedis.get(key);
        if (value != null) {
          samplingSize = samplingSize + value.getBytes().length;
        }
      }
    }
    long samplingAverage = samplingSize / 10;
    // db size
    long dbSize = jedis.dbSize();
    jedis.quit();
    return dbSize * samplingAverage;
  }

  private class PubSubReaderImpl implements PubSubReader {

    private final Logger log = LoggerFactory.getLogger(PubSubReaderImpl.class);

    private final BlockingQueue<String> queue;
    private final List<String> channels;
    private final List<String> patterns;

    private Jedis jedis;
    private JedisPubSub jedisPubSubCallback;

    public PubSubReaderImpl(BlockingQueue<String> queue, List<String> channels,
                            List<String> patterns) {
      this.queue = queue;
      this.channels = channels;
      this.patterns = patterns;
    }

    @Override
    public void start() {
      log.debug("Starting Redis PubSub reader");
      jedis = connection.connect();
      jedisPubSubCallback = new JedisPubSub() {
        @Override
        public void onMessage(String channel, String message) {
          try {
            queue.put(message);
          } catch (Exception e) {
            log.warn("Can't put into the blocking queue", e);
          }
        }

        @Override
        public void onPMessage(String pattern, String channel, String message) {
          try {
            queue.put(message);
          } catch (Exception e) {
            log.warn("Can't put into the blocking queue", e);
          }
        }
      };
      if (channels != null) {
        new Thread() {
          @Override
          public void run() {
            jedis.subscribe(jedisPubSubCallback, channels.toArray
                (new String[channels.size()]));
          }
        }.start();
      }
      if (patterns != null) {
        new Thread() {
          @Override
          public void run() {
            jedis.psubscribe(jedisPubSubCallback, patterns
                .toArray(new String[patterns.size()]));
          }
        }.start();
      }
    }

    @Override
    public void close() {
      log.debug("Closing Redis PubSub reader");
      jedisPubSubCallback.unsubscribe();
      jedisPubSubCallback.punsubscribe();
      try {
        jedis.quit();
      } catch (Exception e) {
        // nothing to do
      }
    }

  }

  @Override
  public PubSubReader createPubSubReader(List<String> channels, List<String> patterns,
                                         BlockingQueue<String> queue) {
    return new PubSubReaderImpl(queue, channels, patterns);
  }

  private class PubSubWriterImpl implements PubSubWriter {

    private final List<String> channels;

    private Jedis jedis;

    public PubSubWriterImpl(List<String> channels) {
      this.channels = channels;
    }

    @Override
    public void start() {
      jedis = connection.connect();
    }

    @Override
    public void publish(String record) {
      for (String channel : channels) {
        jedis.publish(channel, record);
      }
    }

    @Override
    public void close() {
      jedis.quit();
    }

  }

  @Override
  public PubSubWriter createPubSubWriter(List<String> channels) {
    return new PubSubWriterImpl(channels);
  }

}
