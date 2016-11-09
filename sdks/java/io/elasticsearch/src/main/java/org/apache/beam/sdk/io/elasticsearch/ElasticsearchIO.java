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

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.BulkResult;
import io.searchbox.core.Index;
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
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * <p>IO to read and write data on Elasticsearch.</p>
 *
 * <h3>Reading from Elasticsearch</h3>
 *
 * <p>ElasticsearchIO source returns a bounded collection of String representing JSON document
 * as {@code PCollection<String>}.</p>
 *
 * <p>To configure the Elasticsearch source, you have to provide the HTTP address of the
 * instance, and an index name. The following example illustrates various options for
 * configuring the source:</p>
 *
 * <pre>{@code
 *
 * pipeline.apply(ElasticsearchIO.read()
 *   .withAddress("http://host:9200")
 *   .withIndex("my-index")
 *
 * }</pre>
 *
 * <p>The source also accepts optional configuration: {@code withUsername()}, {@code
 * withPassword()}, {@code withQuery()}, {@code withType()}.</p>
 *
 * <h3>Writing to Elasticsearch</h3>
 *
 * <p>ElasticsearchIO supports sink to write documents (as JSON String).</p>
 *
 * <p>To configure Elasticsearch sink, you must specify HTTP {@code address} of the instance, an
 * {@code index}, {@code type}. For instance:</p>
 *
 * <pre>{@code
 *
 *  pipeline
 *    .apply(...)
 *    .apply(ElasticsearchIO.write()
 *      .withAddress("http://host:9200")
 *      .withIndex("my-index")
 *      .withType("my-type")
 *
 * }</pre>
 */
public class ElasticsearchIO {

  public static Write write() {
    return new Write(new Write.Writer(null, null, null, null, null, 1000L, 5));
  }

  public static Read read() {
    return new Read(new BoundedElasticsearchSource(null, null, null, null, null, null,
                                                   null, null));
  }

  private ElasticsearchIO() {
  }

  /**
   * A {@link PTransform<PBegin, PCollection<String>>} reading data from Elasticsearch.
   */
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

  /**
   * A {@link BoundedSource} reading from Elasticsearch.
   */
  protected static class BoundedElasticsearchSource extends BoundedSource<String> {

    private final String address;
    @Nullable
    private final String username;
    @Nullable
    private final String password;
    @Nullable
    private final String query;
    private final String index;
    private final String type;
    @Nullable
    private final Integer nbSlices;
    @Nullable
    private final Integer sliceId;

    private BoundedElasticsearchSource(String address, String username, String password,
                                       String query, String index, String type,
                                       Integer nbSlices,
                                       Integer sliceId) {
      this.address = address;
      this.username = username;
      this.password = password;
      this.query = query;
      this.index = index;
      this.type = type;
      this.nbSlices = nbSlices;
      this.sliceId = sliceId;
    }

    public BoundedElasticsearchSource withAddress(String address) {
      return new BoundedElasticsearchSource(address, username, password, query, index, type,
                                            nbSlices, sliceId);
    }

    public BoundedElasticsearchSource withUsername(String username) {
      return new BoundedElasticsearchSource(address, username, password, query, index, type,
                                            nbSlices, sliceId);
    }

    public BoundedElasticsearchSource withPassword(String password) {
      return new BoundedElasticsearchSource(address, username, password, query, index, type,
                                            nbSlices, sliceId);
    }

    public BoundedElasticsearchSource withQuery(String query) {
      return new BoundedElasticsearchSource(address, username, password, query, index, type,
                                            nbSlices, sliceId);
    }

    public BoundedElasticsearchSource withIndex(String index) {
      return new BoundedElasticsearchSource(address, username, password, query, index, type,
                                            nbSlices, sliceId);
    }

    public BoundedElasticsearchSource withType(String type) {
      return new BoundedElasticsearchSource(address, username, password, query, index, type,
                                            nbSlices, sliceId);
    }

    public BoundedElasticsearchSource withNbSlices(Integer nbSlices) {
      return new BoundedElasticsearchSource(address, username, password, query, index, type,
                                            nbSlices, sliceId);
    }

    public BoundedElasticsearchSource withSliceId(Integer sliceId) {
      return new BoundedElasticsearchSource(address, username, password, query, index, type,
                                            nbSlices, sliceId);
    }

    private RestClient createClient() throws MalformedURLException {

      URL url = new URL(address);
      RestClientBuilder restClientBuilder = RestClient.builder(
          new HttpHost(url.getHost(), url.getPort(), url.getProtocol()));
      if (username != null) {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                                           new UsernamePasswordCredentials(username, password));

        restClientBuilder.setHttpClientConfigCallback(
            new RestClientBuilder.HttpClientConfigCallback() {
              @Override
              public HttpAsyncClientBuilder customizeHttpClient(
                  HttpAsyncClientBuilder httpClientBuilder) {
                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
              }
            });
      }
      return restClientBuilder.build();
    }

    @Override
    public List<? extends BoundedSource<String>> splitIntoBundles(long desiredBundleSizeBytes,
                                                                  PipelineOptions options)
        throws Exception {
      List<BoundedElasticsearchSource> sources = new ArrayList<>();

      long indexSize = getEstimatedSizeBytes(options);
      float nbBundlesFloat = (float) indexSize / desiredBundleSizeBytes;
      int nbBundles = (int) Math.ceil(nbBundlesFloat);
      //ES slice api imposes that number of slices is <= 1024 even if it can be overloaded
      // ES side by conf, we don't want to impose a configuration change to the user
      if (nbBundles > 1024)
        nbBundles = 1024;
      if (nbBundles <= 1) {
        // read all docs in the shard
        sources.add(this);
      } else {
        // split the index into nbBundles chunks of desiredBundleSizeBytes by creating
        // nbBundles sources
        for (int i = 0; i < nbBundles; i++) {
          sources.add(
              this.withNbSlices
                  (nbBundles).withSliceId(i));
        }
      }
      return sources;
    }

    private JsonObject getStats(boolean shardLevel) throws IOException {
      RestClient client = createClient();
      Requester requester = new Requester(client);
      requester.setScheme("GET");
      requester.setEndPoint(String.format("/%s/_stats", index));
      if (shardLevel) {
        requester.setParameter("level", "shards");
      }
      JsonObject jsonObject = requester.performRequest();
      client.close();
      return jsonObject;
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) {

      try {
        JsonObject stats = getStats(false);
        JsonObject indexStats =
            stats.getAsJsonObject("indices").getAsJsonObject(index).getAsJsonObject("primaries");
        JsonObject store = indexStats.getAsJsonObject("store");
        return store.getAsJsonPrimitive("size_in_bytes").getAsLong();
      } catch (IOException e) {
        return 0;
      }
    }

    //protected to be callable from test
    protected long getAverageDocSize() throws IOException {
      JsonObject stats = getStats(false);
      JsonObject indexStats =
          stats.getAsJsonObject("indices").getAsJsonObject(index).getAsJsonObject
              ("primaries");
      JsonObject store = indexStats.getAsJsonObject("store");
      long sizeOfIndex = store.getAsJsonPrimitive("size_in_bytes").getAsLong();
      JsonObject docs = indexStats.getAsJsonObject("docs");
      long nbDocsInIndex = docs.getAsJsonPrimitive("count").getAsLong();
      return sizeOfIndex / nbDocsInIndex;

    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      builder.add(DisplayData.item("address", address));
      builder.add(DisplayData.item("index", index));
      builder.add(DisplayData.item("type", type));
      builder.addIfNotNull(DisplayData.item("sliceId", sliceId));
      builder.addIfNotNull(DisplayData.item("query", query));
      builder.addIfNotNull(DisplayData.item("nbSlices", nbSlices));
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
      checkNotNull(address, "address is a mandatory parameter");
      checkNotNull(index, "index is a mandatory parameter");
      checkNotNull(type, "type is a mandatory parameter");
    }

    @Override
    public Coder<String> getDefaultOutputCoder() {
      return StringUtf8Coder.of();
    }
  }

  private static class Requester {

    private String query;
    private String sliceParams;
    private Map<String, String> params;
    private String endPoint;
    private String scheme;
    private RestClient client;

    public void setEndPoint(String endPoint) {
      this.endPoint = endPoint;
    }

    public void setScheme(String scheme) {
      this.scheme = scheme;
    }

    public void setParameter(String key, String value) {
      params.put(key, value);
    }

    public void setQuery(String query) {
      this.query = query;
    }

    public void setSlice(int sliceId, int maxSlice) {
      sliceParams = String.format("\"slice\": {\n"
                                                     + "    \"id\": %d,\n"
                                                     + "    \"max\": %d\n"
                                                     + "  }", sliceId, maxSlice);
      }

    public Requester(RestClient client) {
      this.client = client;
      this.params = new HashMap<>();
    }

    public JsonObject performRequest() throws IOException {

      StringBuilder requestBody = new StringBuilder();
      if (sliceParams != null){
        requestBody.append(sliceParams);
      }
      if (requestBody.length() > 0){
        requestBody.append(",");
      }
      if (query != null){
        requestBody.append(query);
      }

      Response response;
      if (requestBody.length() > 0) {
        NStringEntity entity = new NStringEntity(String.format("{%d}", requestBody.toString()),
                                                 ContentType
                                                     .APPLICATION_JSON);
        response =
            client.performRequest(scheme, endPoint, params, entity);
      }
      else{
        response = client.performRequest(scheme, endPoint, params);
      }

      InputStream content = response.getEntity().getContent();
      InputStreamReader inputStreamReader = new InputStreamReader(content, "UTF-8");
      JsonObject jsonObject = new Gson().fromJson(inputStreamReader, JsonObject.class);
      return jsonObject;

    }

  }

  private static class BoundedElasticsearchReader extends BoundedSource.BoundedReader<String> {

    private final BoundedElasticsearchSource source;

    private RestClient client;
    private String current;
    private Requester requester;
    private List<String> slice;

    public BoundedElasticsearchReader(BoundedElasticsearchSource source) {
      this.source = source;
    }

    @Override
    public boolean start() throws IOException {
      client = source.createClient();
      String query = source.query;
      if (query == null) {
        query = "  \"query\": {\n"
            + "    \"match_all\": {}\n"
            + "  }\n";
      }
      requester = new Requester(client);
      requester.setQuery(query);
      //TODO when merge with JB, use scroll keepalive given by user
      requester.setParameter("scroll", "1m");
      requester.setEndPoint(String.format("/%s/%s/%s", source.index, source.type, "_search"));
      requester.setScheme("GET");
      requester.setSlice(source.sliceId, source.nbSlices);
      JsonArray hits =
          requester.performRequest().getAsJsonObject("hits").getAsJsonArray("hits");
      //all docs of a slice will be in memory for the advance() to be able to step one doc by one
      // doc (no out of memory because slice size is <= desiredBundleSize)
      //Scroll is mandatory to read huge volume of data and from/size API cannot be used in
      // conjunction with scroll.
      slice = new ArrayList<>();
      for (int i = 0; i < hits.size(); i++) {
        slice.add(hits.get(i).getAsJsonObject().getAsJsonObject("_source").toString());
      }
      return advance();
    }

    @Override
    public boolean advance() throws IOException {
      if (slice != null && slice.size() > 0) {
        current = slice.remove(0);
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
        client.close();
      }
    }

    @Override
    public BoundedSource<String> getCurrentSource() {
      return source;
    }
  }

  /**
   * A {@link PTransform<PCollection<String>, PDone>} writing data to Elasticsearch.
   */
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

    public Write withBatchSize(long batchSize) {
      return new Write(writer.withBatchSize(batchSize));
    }

    public Write withBatchSizeMegaBytes(int batchSizeMegaBytes) {
      return new Write(writer.withBatchSizeMegaBytes(batchSizeMegaBytes));
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
      //byte size of bacth in MB
      private final int batchSizeMegaBytes;

      private JestClient client;
      private ArrayList<Index> batch;
      private long currentBatchSizeBytes;

      public Writer(String address, String username, String password, String index,
                    String type, long batchSize, int batchSizeMegaBytes) {
        this.address = address;
        this.username = username;
        this.password = password;
        this.index = index;
        this.type = type;
        this.batchSize = batchSize;
        this.batchSizeMegaBytes = batchSizeMegaBytes;
      }

      public Writer withAddress(String address) {
        return new Writer(address, username, password, index, type, batchSize,
                          batchSizeMegaBytes);
      }

      public Writer withUsername(String username) {
        return new Writer(address, username, password, index, type, batchSize,
                          batchSizeMegaBytes);
      }

      public Writer withPassword(String password) {
        return new Writer(address, username, password, index, type, batchSize,
                          batchSizeMegaBytes);
      }

      public Writer withIndex(String index) {
        return new Writer(address, username, password, index, type, batchSize,
                          batchSizeMegaBytes);
      }

      public Writer withType(String type) {
        return new Writer(address, username, password, index, type, batchSize,
                          batchSizeMegaBytes);
      }

      public Writer withBatchSize(long batchSize) {
        return new Writer(address, username, password, index, type, batchSize,
                          batchSizeMegaBytes);
      }

      public Writer withBatchSizeMegaBytes(int batchSizeMegaBytes) {
        return new Writer(address, username, password, index, type, batchSize,
                          batchSizeMegaBytes);
      }

      public void validate() {
        checkNotNull(address, "address");
        checkNotNull(index, "index");
        checkNotNull(type, "type");
      }

      @Setup
      public void createClient() throws Exception {
        if (client == null) {
          HttpClientConfig.Builder builder = new HttpClientConfig.Builder(address)
              //              .maxConnectionIdleTime(10, TimeUnit.SECONDS)
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
        currentBatchSizeBytes = 0;
      }

      @ProcessElement
      public void processElement(ProcessContext context) throws Exception {
        String json = context.element();
        batch.add(new Index.Builder(json).index(index).type(type).build());
        currentBatchSizeBytes += json.getBytes().length;
        if (batch.size() >= batchSize
            || currentBatchSizeBytes >= (batchSizeMegaBytes * 1024 * 1024)) {
          finishBundle(context);
        }
      }

      @FinishBundle
      public void finishBundle(Context context) throws Exception {
        if (batch.size() > 0) {
          Bulk bulk = new Bulk.Builder()
              .defaultIndex(index)
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
