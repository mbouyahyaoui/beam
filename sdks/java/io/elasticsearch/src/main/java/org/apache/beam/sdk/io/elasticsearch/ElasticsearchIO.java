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
package org.apache.beam.sdk.io.elasticsearch;

import com.google.common.base.Preconditions;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.BulkResult;
import io.searchbox.core.Index;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

/**
 * TODO
 */
public class ElasticsearchIO {

  public static Write write() {
    return new Write(new Write.Writer(null, null, null, null, null, 1024L));
  }

  public static Read read() {
    return new Read(new BoundedElasticsearchSource(null, null, null, null, null, null));
  }

  private ElasticsearchIO() {
  }

  public static class Read extends PTransform<PBegin, PCollection<String>> {

    public Read withAddress(String address) {
      return new Read(source.withAddress(address));
    }

    public Read withUsername(String username) {
      return new Read(source.withUsername(username));
    }

    public Read withPassword(String password) {
      return new Read(source.withPassword(password));
    }

    public Read withQuery(String query) {
      return new Read(source.withQuery(query));
    }

    public Read withIndex(String index) {
      return new Read(source.withIndex(index));
    }

    public Read withType(String type) {
      return new Read(source.withType(type));
    }

    private final BoundedElasticsearchSource source;

    private Read(BoundedElasticsearchSource source) {
      this.source = source;
    }

    @Override
    public PCollection<String> apply(PBegin input) {
      return input.apply(org.apache.beam.sdk.io.Read.from(getSource()));
    }

    public BoundedElasticsearchSource getSource() {
      return source;
    }

  }

  private static class BoundedElasticsearchSource extends BoundedSource<String> {

    private final String address;
    @Nullable
    private final String username;
    @Nullable
    private final String password;
    @Nullable
    private final String query;
    @Nullable
    private final String index;
    @Nullable
    private final String type;

    private BoundedElasticsearchSource(String address, String username, String password,
                                       String query, String index, String type) {
      this.address = address;
      this.username = username;
      this.password = password;
      this.query = query;
      this.index = index;
      this.type = type;
    }

    public BoundedElasticsearchSource withAddress(String address) {
      return new BoundedElasticsearchSource(address, username, password, query, index, type);
    }

    public BoundedElasticsearchSource withUsername(String username) {
      return new BoundedElasticsearchSource(address, username, password, query, index, type);
    }

    public BoundedElasticsearchSource withPassword(String password) {
      return new BoundedElasticsearchSource(address, username, password, query, index, type);
    }

    public BoundedElasticsearchSource withQuery(String query) {
      return new BoundedElasticsearchSource(address, username, password, query, index, type);
    }

    public BoundedElasticsearchSource withIndex(String index) {
      return new BoundedElasticsearchSource(address, username, password, query, index, type);
    }

    public BoundedElasticsearchSource withType(String type) {
      return new BoundedElasticsearchSource(address, username, password, query, index, type);
    }

    @Override
    public List<? extends BoundedSource<String>> splitIntoBundles(long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
      // TODO
      return null;
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
      // TODO
      return 0;
    }

    @Override
    public boolean producesSortedKeys(PipelineOptions options) throws Exception {
      return false;
    }

    @Override
    public BoundedReader<String> createReader(PipelineOptions options) throws IOException {
      return new BoundedElasticsearchReader(this);
    }

    @Override
    public void validate() {
      Preconditions.checkNotNull(address, "address");
    }

    @Override
    public Coder<String> getDefaultOutputCoder() {
      return StringUtf8Coder.of();
    }
  }

  private static class BoundedElasticsearchReader extends BoundedSource.BoundedReader<String> {

    private final BoundedElasticsearchSource source;

    private JestClient client;
    private List<String> result;
    private String current;

    public BoundedElasticsearchReader(BoundedElasticsearchSource source) {
      this.source = source;
    }

    @Override
    public boolean start() throws IOException {
      HttpClientConfig.Builder builder = new HttpClientConfig.Builder(source.address)
          .multiThreaded(true);
      if (source.username != null) {
        builder = builder.defaultCredentials(source.username, source.password);
      }
      JestClientFactory factory = new JestClientFactory();
      factory.setHttpClientConfig(builder.build());
      client = factory.getObject();

      String query = source.query;
      if (query == null) {
        query = "{\n"
            + "  \"query\": {\n"
            + "    \"match_all\": {}\n"
            + "  }\n"
            + "}";
      }

      Search.Builder searchBuilder = new Search.Builder(query);
      if (source.index != null) {
        searchBuilder.addIndex(source.index);
      }
      if (source.type != null) {
        searchBuilder.addType(source.type);
      }
      Search search = searchBuilder.build();

      SearchResult searchResult = client.execute(search);
      result = searchResult.getSourceAsStringList();

      return advance();
    }

    @Override
    public boolean advance() throws IOException {
      if (result != null && result.size() > 0) {
        current = result.remove(0);
        return true;
      } else {
        return false;
      }
    }

    @Override
    public String getCurrent() throws NoSuchElementException {
      return current;
    }

    @Override
    public void close() throws IOException {
      if (client != null) {
        client.shutdownClient();
      }
    }

    @Override
    public BoundedSource<String> getCurrentSource() {
      return source;
    }
  }

  public static class Write extends PTransform<PCollection<String>, PDone> {

    public Write withAddress(String address) {
      return new Write(writer.withAddress(address));
    }

    public Write withUsername(String username) {
      return new Write(writer.withUsername(username));
    }

    public Write withPassword(String password) {
      return new Write(writer.withPassword(password));
    }

    public Write withIndex(String index) {
      return new Write(writer.withIndex(index));
    }

    public Write withType(String type) {
      return new Write(writer.withType(type));
    }

    private final Writer writer;

    private Write(Writer writer) {
      this.writer = writer;
    }

    @Override
    public PDone apply(PCollection<String> input) {
      input.apply(ParDo.of(writer));
      return PDone.in(input.getPipeline());
    }

    private static class Writer extends DoFn<String, Void> {

      private final String address;
      private final String username;
      private final String password;
      private final String index;
      private final String type;
      private final long batchSize;

      private JestClient client;
      private ArrayList<Index> batch;

      public Writer(String address, String username, String password, String index, String type,
                    long batchSize) {
        this.address = address;
        this.username = username;
        this.password = password;
        this.index = index;
        this.type = type;
        this.batchSize = batchSize;
      }

      public Writer withAddress(String address) {
        return new Writer(address, username, password, index, type, batchSize);
      }

      public Writer withUsername(String username) {
        return new Writer(address, username, password, index, type, batchSize);
      }

      public Writer withPassword(String password) {
        return new Writer(address, username, password, index, type, batchSize);
      }

      public Writer withIndex(String index) {
        return new Writer(address, username, password, index, type, batchSize);
      }

      public Writer withType(String type) {
        return new Writer(address, username, password, index, type, batchSize);
      }

      public void validate() {
        Preconditions.checkNotNull(address, "address");
        Preconditions.checkNotNull(index, "index");
        Preconditions.checkNotNull(type, "type");
      }

      @Setup
      public void createClient() throws Exception {
        if (client == null) {
          HttpClientConfig.Builder builder = new HttpClientConfig.Builder(address)
              .multiThreaded(true);
          if (username != null) {
            builder = builder.defaultCredentials(username, password);
          }
          JestClientFactory factory = new JestClientFactory();
          factory.setHttpClientConfig(builder.build());
          client = factory.getObject();
        }
      }

      @StartBundle
      public void startBundle(Context context) throws Exception {
        batch = new ArrayList<>();
      }

      @ProcessElement
      public void processElement(ProcessContext context) throws Exception {
        String json = context.element();
        batch.add(new Index.Builder(json).index(index).type(type).build());
        if (batch.size() >= batchSize) {
          finishBundle(context);
        }
      }

      @FinishBundle
      public void finishBundle(Context context) throws Exception {
        Bulk bulk = new Bulk.Builder()
            .defaultType(index)
            .defaultType(type)
            .addAction(batch)
            .build();
        BulkResult result = client.execute(bulk);
        if (!result.isSucceeded()) {
          for (BulkResult.BulkResultItem item : result.getFailedItems()) {
            System.out.println(item.toString());
          }
          throw new IllegalStateException("Can't update Elasticsearch: "
              + result.getErrorMessage());
        }
        batch.clear();
      }

      @Teardown
      public void closeClient() throws Exception {
        if (client != null) {
          client.shutdownClient();
        }
      }

    }
  }

}
