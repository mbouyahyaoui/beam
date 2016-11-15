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
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

/**
 * <p>IO to read and write data on Elasticsearch.
 *
 * <h3>Reading from Elasticsearch</h3>
 *
 * <p>ElasticsearchIO source returns a bounded collection of String representing JSON document
 * as {@code PCollection<String>}.
 *
 * <p>To configure the Elasticsearch source, you have to provide a connection configuration
 * containing the HTTP address of the instance, an index name and a type. The following example
 * illustrates various options for configuring the source:
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
 * {@code withPassword()}.
 *
 * <p>You can also specify a query on the {@code read()} using {@code withQuery()}.
 *
 * <h3>Writing to Elasticsearch</h3>
 *
 * <p>ElasticsearchIO supports sink to write documents (as JSON String).
 *
 * <p>To configure Elasticsearch sink, similar to the read, you have to provide a connection
 * configuration. For instance:
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
 * to specify the size of the write batch.
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

  private static JsonObject parseResponse(Response response) throws IOException {
    InputStream content = response.getEntity().getContent();
    InputStreamReader inputStreamReader = new InputStreamReader(content, "UTF-8");
    JsonObject jsonObject = new Gson().fromJson(inputStreamReader, JsonObject.class);
    return jsonObject;
  }

  /**
   * A POJO describing a connection configuration to Elasticsearch.
   */
  @AutoValue
  public abstract static class ConnectionConfiguration implements Serializable {

    abstract String getAddress();

    @Nullable
    abstract String getUsername();

    @Nullable
    abstract String getPassword();

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
      checkNotNull(address, "address is required");
      checkNotNull(index, "index name is required");
      checkNotNull(type, "type is required");
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

    private RestClient createClient() throws MalformedURLException {
      URL url = new URL(getAddress());
      RestClientBuilder restClientBuilder =
          RestClient.builder(new HttpHost(url.getHost(), url.getPort(), url.getProtocol()));
      if (getUsername() != null) {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                                           new UsernamePasswordCredentials(getUsername(),
                                                                           getPassword()));
        restClientBuilder.setHttpClientConfigCallback(
            new RestClientBuilder.HttpClientConfigCallback() {
              public HttpAsyncClientBuilder customizeHttpClient(
                  HttpAsyncClientBuilder httpAsyncClientBuilder) {
                return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
              }
            });
      }
      return restClientBuilder.build();
    }

  }

  /**
   * A {@link PTransform} reading data from Elasticsearch.
   */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<String>> {
    @Nullable
    abstract ConnectionConfiguration getConnectionConfiguration();

    @Nullable
    abstract String getQuery();

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
          new BoundedElasticsearchSource(this, null)
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

    protected BoundedElasticsearchSource(Read spec, String shardPreference) {
      this.spec = spec;
      this.shardPreference = shardPreference;
    }

    public BoundedElasticsearchSource withShardPreference(String shardPreference) {
      return new BoundedElasticsearchSource(spec, shardPreference);
    }

    @Override
    public List<? extends BoundedSource<String>> splitIntoBundles(long desiredBundleSizeBytes,
                                                                  PipelineOptions options)
        throws Exception {
      List<BoundedElasticsearchSource> sources = new ArrayList<>();

      // we split per shard
      // unfortunately, Elasticsearch 2.x doesn't provide a way to do parallel reads on a single
      // shard
      JsonObject statsJson = getStats(true);
      JsonObject shardsJson = statsJson.getAsJsonObject("indices")
          .getAsJsonObject(spec.getConnectionConfiguration().getIndex())
          .getAsJsonObject("shards");
      Set<Map.Entry<String, JsonElement>> shards = shardsJson.entrySet();
      for (Map.Entry<String, JsonElement> shardJson : shards) {
        String shardId = shardJson.getKey();
        sources.add(new BoundedElasticsearchSource(spec, shardId));
      }
      return sources;
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws IOException{
      // as Elasticsearch 2.x doesn't not support any way to do parallel read inside a shard
      // the estimated size bytes is not really used in the split into bundles.
      // However, we implement this method anyway as the runners can use it.
      // NB: Elasticsearch 5.x now provides the slice API.
        JsonObject statsJson = getStats(false);
        JsonObject indexStats =
            statsJson.getAsJsonObject("indices").getAsJsonObject(spec.getConnectionConfiguration().getIndex()).getAsJsonObject("primaries");
        JsonObject store = indexStats.getAsJsonObject("store");
        return store.getAsJsonPrimitive("size_in_bytes").getAsLong();
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      spec.populateDisplayData(builder);
      builder.addIfNotNull(DisplayData.item("shard", shardPreference));
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

    private JsonObject getStats(boolean shardLevel) throws IOException {
      RestClient restClient = spec.getConnectionConfiguration().createClient();
      HashMap<String,String> params = new HashMap<>();
      if (shardLevel) {
        params.put("level", "shards");
      }
      String endpoint = String.format("/%s/_stats",
                                    spec.getConnectionConfiguration
                                        ().getIndex());
      Response response = restClient.performRequest("GET", endpoint, params, null, null);
      restClient.close();
      return parseResponse(response);

    }

  }

  private static class BoundedElasticsearchReader extends BoundedSource.BoundedReader<String> {

    private final BoundedElasticsearchSource source;

    private RestClient restClient;
    private String current;
    private String scroll;

    public BoundedElasticsearchReader(BoundedElasticsearchSource source) {
      this.source = source;
    }

    @Override
    public boolean start() throws IOException {
      restClient = source.spec.getConnectionConfiguration().createClient();

      String query = source.spec.getQuery();
      if (query == null) {
        query = "{\n"
            + "  \"query\": {\n"
            + "    \"match_all\": {}\n"
            + "  }\n"
            + "}";
      }

      Response response;
      String endPoint = String.format("/%s/%s/_search",
                                 source.spec.getConnectionConfiguration().getIndex(),
                                 source.spec.getConnectionConfiguration().getType());
      Map<String, String> params = new HashMap<>();
      //TODO pass keepalive
      params.put("scroll", "1m");
      params.put("size", "1");
      if (source.shardPreference != null) {
        params.put("preference", "_shard:" + source.shardPreference);
      }
      HttpEntity queryEntity = new NStringEntity(query, ContentType.APPLICATION_JSON);
      response = restClient.performRequest("GET", endPoint, params, queryEntity);
      JsonObject searchResult = parseResponse(response);
      scroll = searchResult.getAsJsonPrimitive("_scroll_id").getAsString();
      return updateCurrent(searchResult);
    }

    @Override
    public boolean advance() throws IOException {
      HttpEntity scrollEntity = new NStringEntity("{"
                                                      + "\"scroll\" : \"1m\","
                                                      + "\"scroll_id\" : \"" + scroll + "\""
                                                      + "}", ContentType.APPLICATION_JSON);

      Response response = restClient.performRequest("GET", "/_search/scroll", null, scrollEntity);
      JsonObject searchResult = parseResponse(response);
      return updateCurrent(searchResult);
    }

    private boolean updateCurrent(JsonObject searchResult) {
      //stop if no more data
      if (searchResult.getAsJsonObject("hits").getAsJsonArray("hits").size() == 0) {
        return false;
      }
      current = searchResult.getAsJsonObject("hits").getAsJsonArray("hits").get(0)
          .getAsJsonObject().getAsJsonObject("_source").toString();
      return true;
    }

    @Override
    public String getCurrent() throws NoSuchElementException {
      return current;
    }

    @Override
    public void close() throws IOException {
      // remove the scroll
      HttpEntity entity = new NStringEntity("{ \"scroll_id\" : [\"" + scroll + "\"] }",
                                            ContentType.APPLICATION_JSON);
      restClient.performRequest("DELETE", "_search/scroll",
                                Collections.<String, String>emptyMap(),
                                entity);
      // close client
      if (restClient != null) {
        restClient.close();
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

    @Nullable
    abstract ConnectionConfiguration getConnectionConfiguration();

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

      private RestClient client;
      //TODO recode batch
//      private ArrayList<Index> batch;
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
        //TODO recode batch
/*
        batch = new ArrayList<>();
        currentBatchSizeBytes = 0;
*/
      }

      @ProcessElement
      public void processElement(ProcessContext context) throws Exception {
        //TODO recode batch
/*
        String json = context.element();

        String create =

            batch.add(new Index.Builder(json).index(spec.getConnectionConfiguration().getIndex())
                          .type(spec.getConnectionConfiguration().getType())
                          .build());
        currentBatchSizeBytes += json.getBytes().length;
        if (batch.size() >= spec.getBatchSize()
            || currentBatchSizeBytes >= (spec.getBatchSizeMegaBytes() * 1024 * 1024)) {
          finishBundle(context);
        }
*/
      }

      @FinishBundle
      public void finishBundle(Context context) throws Exception {
        //TODO recode batch
/*
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
*/
      }

      @Teardown
      public void closeClient() throws Exception {
        if (client != null) {
          client.close();
        }
      }

    }
  }

}
