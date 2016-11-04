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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.BulkResult;
import io.searchbox.core.Index;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.indices.Stats;
import io.searchbox.params.Parameters;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

/**
 * <p>IO to read and write data on Elasticsearch.</p>
 *
 * <h3>Reading from Elasticsearch</h3>
 *
 * <p>ElasticsearchIO source returns a bounded collection of String representing JSON document
 * as {@code PCollection<String>}.</p>
 *
 * <p>To configure the Elasticsearch source, you have to provide a connection configuration
 * containing the HTTP address of the instance, an index name and a type. The following example
 * illustrates various options for configuring the source:</p>
 *
 * <pre>{@code
 *
 * pipeline.apply(ElasticsearchIO.read().withConnectionConfiguration(
 *    ElasticsearchIO.ConnectionConfiguration.create("http://host:9200", "my-index", "my-type")
 * )
 *
 * }</pre>
 *
 * <p>The connection configuration also accepts optional configuration: {@code withUsername()} and
 * {@code withPassword()}.</p>
 *
 * <p>You can also specify a query on the {@code read()} using {@code withQuery()}.</p>
 *
 * <h3>Writing to Elasticsearch</h3>
 *
 * <p>ElasticsearchIO supports sink to write documents (as JSON String).</p>
 *
 * <p>To configure Elasticsearch sink, similar to the read, you have to provide a connection
 * configuration. For instance:</p>
 *
 * <pre>{@code
 *
 *  pipeline
 *    .apply(...)
 *    .apply(ElasticsearchIO.write().withConnectionConfiguration(
 *       ElasticsearchIO.ConnectionConfiguration.create("http://host:9200", "my-index", "my-type")
 *    )
 *
 * }</pre>
 *
 * <p>Optionally, you can provide {@code withBatchSize()} and {@code withBatchSizeMegaBytes()}
 * to specify the size of the write batch.</p>
 */
public class ElasticsearchIO {

  public static Read read() {
    return new AutoValue_ElasticsearchIO_Read.Builder().build();
  }

  public static Write write() {
    return new AutoValue_ElasticsearchIO_Write.Builder().setBatchSize(1000L)
        .setBatchSizeMegaBytes(5).build();
  }

  private ElasticsearchIO() {
  }

  /**
   * A POJO describing a connection configuration to Elasticsearch.
   */
  @AutoValue
  public abstract static class ConnectionConfiguration implements Serializable {

    abstract String getAddress();
    @Nullable abstract String getUsername();
    @Nullable abstract String getPassword();
    abstract String getIndex();
    abstract String getType();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setAddress(String address);
      abstract Builder setUsername(String username);
      abstract Builder setPassword(String password);
      abstract Builder setIndex(String index);
      abstract Builder setType(String type);
      abstract ConnectionConfiguration build();
    }

    public static ConnectionConfiguration create(String address, String index, String type) {
      checkNotNull(address, "address");
      checkNotNull(index, "index");
      checkNotNull(type, "type");
      return new AutoValue_ElasticsearchIO_ConnectionConfiguration.Builder()
          .setAddress(address)
          .setIndex(index)
          .setType(type)
          .build();
    }

    public ConnectionConfiguration withUsername(String username) {
      return builder().setUsername(username).build();
    }

    public ConnectionConfiguration withPassword(String password) {
      return builder().setPassword(password).build();
    }

    private void populateDisplayData(DisplayData.Builder builder) {
      builder.add(DisplayData.item("address", getAddress()));
      builder.add(DisplayData.item("index", getIndex()));
      builder.add(DisplayData.item("type", getType()));
      builder.addIfNotNull(DisplayData.item("username", getUsername()));
    }

    private JestClient createClient() {
      HttpClientConfig.Builder builder = new HttpClientConfig.Builder(getAddress())
          //          .maxConnectionIdleTime(10, TimeUnit.SECONDS)
          .multiThreaded(true);
      if (getUsername() != null) {
        builder = builder.defaultCredentials(getUsername(), getPassword());
      }
      JestClientFactory factory = new JestClientFactory();
      factory.setHttpClientConfig(builder.build());
      return factory.getObject();
    }

  }

  /**
   * A {@link PTransform} reading data from Elasticsearch.
   */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<String>> {
    @Nullable abstract ConnectionConfiguration getConnectionConfiguration();
    @Nullable abstract String getQuery();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setConnectionConfiguration(ConnectionConfiguration connectionConfiguration);
      abstract Builder setQuery(String query);
      abstract Read build();
    }

    public Read withConnectionConfiguration(ConnectionConfiguration connectionConfiguration) {
      checkNotNull(connectionConfiguration, "connectionConfiguration");
      return builder().setConnectionConfiguration(connectionConfiguration).build();
    }

    public Read withQuery(String query) {
      checkNotNull(query, "query");
      return builder().setQuery(query).build();
    }

    @Override
    public PCollection<String> apply(PBegin input) {
      return input.apply(org.apache.beam.sdk.io.Read.from(
          new BoundedElasticsearchSource(this, null, null, null)
      ));
    }

    @Override
    public void validate(PBegin input) {
      checkNotNull(getConnectionConfiguration(), "connectionConfiguration");
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.addIfNotNull(DisplayData.item("query", getQuery()));
      getConnectionConfiguration().populateDisplayData(builder);
    }

  }

  /**
   * A {@link BoundedSource} reading from Elasticsearch.
   */
  protected static class BoundedElasticsearchSource extends BoundedSource<String> {

    private ElasticsearchIO.Read spec;
    @Nullable
    private final String shardPreference;
    @Nullable
    private final Long sizeToRead;
    @Nullable
    private final Integer offset;

    protected BoundedElasticsearchSource(Read spec, String shardPreference, Long sizeToRead,
                                       Integer offset) {
      this.spec = spec;
      this.shardPreference = shardPreference;
      this.sizeToRead = sizeToRead;
      this.offset = offset;
    }

    public BoundedElasticsearchSource withShardPreference(String shardPreference) {
      return new BoundedElasticsearchSource(spec, shardPreference, sizeToRead, offset);
    }

    public BoundedElasticsearchSource withSizeToRead(Long sizeToRead) {
      return new BoundedElasticsearchSource(spec, shardPreference, sizeToRead, offset);
    }

    public BoundedElasticsearchSource withOffset(Integer offset) {
      return new BoundedElasticsearchSource(spec, shardPreference, sizeToRead, offset);
    }

    @Override
    public List<? extends BoundedSource<String>> splitIntoBundles(long desiredBundleSizeBytes,
                                                                  PipelineOptions options)
        throws Exception {
      List<BoundedElasticsearchSource> sources = new ArrayList<>();

      JestClient client = spec.getConnectionConfiguration().createClient();
      Stats stats = new Stats.Builder().addIndex(spec.getConnectionConfiguration().getIndex())
          .setParameter("level", "shards").build();
      JestResult result = client.execute(stats);
      client.shutdownClient();

      if (result.isSucceeded()) {
        JsonObject jsonObject = result.getJsonObject();
        JsonObject shardsJson =
            jsonObject.getAsJsonObject("indices")
                .getAsJsonObject(spec.getConnectionConfiguration().getIndex())
                .getAsJsonObject("shards");
        Set<Map.Entry<String, JsonElement>> entries = shardsJson.entrySet();
        for (Map.Entry<String, JsonElement> shardJson : entries) {
          String shardId = shardJson.getKey();
          JsonArray value = (JsonArray) shardJson.getValue();
          long shardSize =
              value.get(0).getAsJsonObject().getAsJsonObject("store").getAsJsonPrimitive(
                  "size_in_bytes").getAsLong();
          String shardPreference = "_shards:" + shardId;
          float nbBundlesFloat = (float) shardSize / desiredBundleSizeBytes;
          int nbBundles = (int) Math.ceil(nbBundlesFloat);
          if (nbBundles <= 1) {
            // read all docs in the shard
            sources.add(this.withShardPreference(shardPreference));
          } else {
            // split the shard into nbBundles chunks of desiredBundleSizeBytes by creating
            // nbBundles sources
            for (int i = 0; i < nbBundles; i++) {
              sources.add(
                  this.withShardPreference(shardPreference).withSizeToRead
                      (desiredBundleSizeBytes).withOffset(i));
            }
          }
        }
      } else {
        sources.add(this);
      }
      return sources;
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws IOException {
      JestClient client = spec.getConnectionConfiguration().createClient();
      Stats stats = new Stats.Builder().addIndex(spec.getConnectionConfiguration().getIndex())
          .build();
      JestResult result = client.execute(stats);
      client.shutdownClient();
      if (result.isSucceeded()) {
        return getIndexSize(result);
      }
      return 0;
    }

    //protected to be callable from test
    protected long getAverageDocSize() throws IOException {
      JestClient client = spec.getConnectionConfiguration().createClient();
      Stats stats = new Stats.Builder().addIndex(spec.getConnectionConfiguration().getIndex())
          .build();
      JestResult result = client.execute(stats);
      client.shutdownClient();
      if (!result.isSucceeded()) {
        throw new IOException("Cannot get average size of doc in index "
            + spec.getConnectionConfiguration().getIndex());
      } else {
        JsonObject jsonResult = result.getJsonObject();
        JsonObject statsJson =
            jsonResult.getAsJsonObject("indices")
                .getAsJsonObject(spec.getConnectionConfiguration().getIndex())
                .getAsJsonObject("primaries");
        JsonObject storeJson = statsJson.getAsJsonObject("store");
        long sizeOfIndex = storeJson.getAsJsonPrimitive("size_in_bytes").getAsLong();
        JsonObject docsJson = statsJson.getAsJsonObject("docs");
        long nbDocsInIndex = docsJson.getAsJsonPrimitive("count").getAsLong();
        return sizeOfIndex / nbDocsInIndex;
      }

    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      spec.populateDisplayData(builder);
      builder.addIfNotNull(DisplayData.item("documents offset", offset));
      builder.addIfNotNull(DisplayData.item("shard", shardPreference));
      builder.addIfNotNull(DisplayData.item("sizeToRead", sizeToRead));
    }

    private long getIndexSize(JestResult result) {
      JsonObject jsonResult = result.getJsonObject();
      JsonObject statsJson =
          jsonResult.getAsJsonObject("indices")
              .getAsJsonObject(spec.getConnectionConfiguration().getIndex())
              .getAsJsonObject("primaries");
      JsonObject storeJson = statsJson.getAsJsonObject("store");
      return storeJson.getAsJsonPrimitive("size_in_bytes").getAsLong();
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
      spec.validate(null);
    }

    @Override
    public Coder<String> getDefaultOutputCoder() {
      return StringUtf8Coder.of();
    }
  }

  private static class BoundedElasticsearchReader extends BoundedSource.BoundedReader<String> {

    private final BoundedElasticsearchSource source;

    private JestClient client;
    private String current;
    private long desiredNbDocs;
    private long nbDocsRead;
    private Search.Builder searchBuilder;

    public BoundedElasticsearchReader(BoundedElasticsearchSource source) {
      this.source = source;
    }

    @Override
    public boolean start() throws IOException {
      /*
      HttpClientConfig.Builder builder = new HttpClientConfig.Builder(
          source.spec.getConnectionConfiguration().getAddress())
          //org.apache.http.NoHttpResponseException: ... failed to respond
          //is still present even after upgrading jest (and http-client) and even after setting
          // maxConnectionIdleTime as recommended
          //https://www.bountysource.com/issues/9650168-jest-and-apache-http-client-4-4-error
          //increasing timeout does not fix the problem either, multi-thread=false either
//          .maxConnectionIdleTime(10, TimeUnit.SECONDS)
//          .readTimeout(20)
          .multiThreaded(true);
      if (source.spec.getConnectionConfiguration() != null) {
        builder = builder.defaultCredentials(source.username, source.password);
      }
      JestClientFactory factory = new JestClientFactory();
      factory.setHttpClientConfig(builder.build());
      client = factory.getObject();
      */
      client = source.spec.getConnectionConfiguration().createClient();

      String query = source.spec.getQuery();
      if (query == null) {
        query = "{\n"
            + "  \"query\": {\n"
            + "    \"match_all\": {}\n"
            + "  }\n"
            + "}";
      }

      searchBuilder = new Search.Builder(query);
      if (source.shardPreference != null) {
        searchBuilder.setParameter("preference", source.shardPreference);
      }
      searchBuilder.setParameter(Parameters.SIZE, 1);
      if (source.sizeToRead != null) {
        //we are in the case of splitting a shard
        nbDocsRead = 0;
        desiredNbDocs = convertBytesToNbDocs(source.sizeToRead);
      }
      searchBuilder.addIndex(source.spec.getConnectionConfiguration().getIndex());
      searchBuilder.addType(source.spec.getConnectionConfiguration().getType());
      return advance();
    }

    private long convertBytesToNbDocs(Long size) throws IOException {
      long averageDocSize = source.getAverageDocSize();
      float nbDocsFloat = (float) size / averageDocSize;
      long nbDocs = (long) Math.ceil(nbDocsFloat);
      return nbDocs;
    }

    @Override
    public boolean advance() throws IOException {
      //stop if we need to split the shard and we have reached the desiredBundleSize
      if ((source.sizeToRead != null) && (nbDocsRead == desiredNbDocs)) {
        return false;
      }
      long from;
      if (source.sizeToRead != null) {
        //we are in the case of splitting a shard
        from = (desiredNbDocs * source.offset) + nbDocsRead;
      } else {
        from = nbDocsRead;
      }
      searchBuilder.setParameter(Parameters.FROM, from);
      Search search = searchBuilder.build();
      SearchResult searchResult = client.execute(search);
      if (!searchResult.isSucceeded()) {
        throw new IOException("cannot perform request on ES");
      }
      //stop if no more data
      if (searchResult.getJsonObject().getAsJsonObject("hits").getAsJsonArray("hits").size() == 0) {
        return false;
      }
      current = searchResult.getSourceAsString();
      nbDocsRead++;
      return true;
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

  /**
   * A {@link PTransform} writing data to Elasticsearch.
   */
  @AutoValue
  public abstract static class Write extends PTransform<PCollection<String>, PDone> {

    @Nullable abstract ConnectionConfiguration getConnectionConfiguration();
    abstract long getBatchSize();
    abstract int getBatchSizeMegaBytes();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setConnectionConfiguration(ConnectionConfiguration connectionConfiguration);
      abstract Builder setBatchSize(long batchSize);
      abstract Builder setBatchSizeMegaBytes(int batchSizeMegaBytes);
      abstract Write build();
    }

    public Write withConnectionConfiguration(ConnectionConfiguration connectionConfiguration) {
      return builder().setConnectionConfiguration(connectionConfiguration).build();
    }

    public Write withBatchSize(long batchSize) {
      return builder().setBatchSize(batchSize).build();
    }

    public Write withBatchSizeMegaBytes(int batchSizeMegaBytes) {
      return builder().setBatchSizeMegaBytes(batchSizeMegaBytes).build();
    }

    @Override
    public void validate(PCollection<String> input) {
      checkNotNull(getConnectionConfiguration(), "connectionConfiguration");
    }

    @Override
    public PDone apply(PCollection<String> input) {
      input.apply(ParDo.of(new WriterFn(this)));
      return PDone.in(input.getPipeline());
    }

    private static class WriterFn extends DoFn<String, Void> {

      private Write spec;

      private JestClient client;
      private ArrayList<Index> batch;
      private long currentBatchSizeBytes;

      public WriterFn(Write spec) {
        this.spec = spec;
      }

      @Setup
      public void createClient() throws Exception {
        if (client == null) {
          client = spec.getConnectionConfiguration().createClient();
        }
      }

      @StartBundle
      public void startBundle(Context context) throws Exception {
        batch = new ArrayList<>();
        currentBatchSizeBytes = 0;
      }

      @ProcessElement
      public void processElement(ProcessContext context) throws Exception {
        String json = context.element();
        batch.add(new Index.Builder(json).index(spec.getConnectionConfiguration().getIndex())
            .type(spec.getConnectionConfiguration().getType())
            .build());
        currentBatchSizeBytes += json.getBytes().length;
        if (batch.size() >= spec.getBatchSize()
            || currentBatchSizeBytes >= (spec.getBatchSizeMegaBytes() * 1024 * 1024)) {
          finishBundle(context);
        }
      }

      @FinishBundle
      public void finishBundle(Context context) throws Exception {
        if (batch.size() > 0) {
          Bulk bulk = new Bulk.Builder()
              .defaultIndex(spec.getConnectionConfiguration().getIndex())
              .defaultType(spec.getConnectionConfiguration().getType())
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
