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

import com.google.gson.JsonObject;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.indices.Stats;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.SourceTestUtils;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.commons.io.FileUtils;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.BoundedElasticsearchSource;
import static org.apache.beam.sdk.testing.SourceTestUtils.readFromSource;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.apache.beam.sdk.testing.SourceTestUtils.assertSourcesEqualReferenceSource;
import static org.apache.beam.sdk.testing.SourceTestUtils.readFromSource;

/**
 * Test on {@link ElasticsearchIO}.
 */
public class ElasticsearchIOTest implements Serializable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchIOTest.class);

  private static final String DATA_DIRECTORY = "target/elasticsearch";
  public static final String ES_INDEX = "beam";
  public static final String ES_TYPE = "test";
  public static final String ES_IP = "localhost";
  public static final String ES_HTTP_PORT = "9201";
  public static final String ES_TCP_PORT = "9301";
  private static final long NB_DOCS = 3L;

  private transient Node node;

  @Before
  public void before() throws Exception {
    LOGGER.info("Starting embedded Elasticsearch instance");
    FileUtils.deleteDirectory(new File(DATA_DIRECTORY));
    Settings.Builder settingsBuilder =
        Settings.settingsBuilder()
            .put("cluster.name", "beam")
            .put("http.enabled", "true")
            .put("node.data", "true")
            .put("path.data", DATA_DIRECTORY)
            .put("path.home", DATA_DIRECTORY)
            .put("node.name", "beam")
            .put("network.host", ES_IP)
            .put("port", ES_TCP_PORT)
            .put("http.port", ES_HTTP_PORT);
    node = NodeBuilder.nodeBuilder().settings(settingsBuilder).build();
    LOGGER.info("Elasticsearch node created");
    if (node != null) {
      node.start();
    }
  }

  private final class BulkListener implements BulkProcessor.Listener {

    @Override
    public void beforeBulk(long l, BulkRequest bulkRequest) {
    }

    @Override
    public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {
    }

    @Override
    public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {
    }
  }

  private long sampleIndex(long nbDocs) throws Exception {
    BulkProcessor bulkProcessor =
        BulkProcessor.builder(node.client(), new BulkListener())
            .setBulkActions(100)
            .setConcurrentRequests(5)
            .setFlushInterval(TimeValue.timeValueSeconds(5))
            .build();
    String[] scientists =
        { "Einstein", "Darwin", "Copernicus", "Pasteur", "Curie", "Faraday", "Newton", "Bohr",
            "Galilei", "Maxwell" };
    long size = 0;
    for (int i = 0; i < nbDocs; i++) {
      int index = i % scientists.length;
      String source = String.format("{\"scientist\":\"%s\", \"id\":%d}", scientists[index], i);
      size += source.getBytes().length;
      bulkProcessor.add(new IndexRequest(ES_INDEX, ES_TYPE).source(source));
    }
    boolean hascompleted = bulkProcessor.awaitClose(6, TimeUnit.SECONDS);
    if (!hascompleted)
      throw (new Exception("the specified waiting time elapsed before all (concurent) bulk "
                               + "requests complete"));
    waitForESIndexationToFinish();
    return size;
  }

  private void waitForESIndexationToFinish() throws Exception {
    while (!isIndexingFinished(NB_DOCS))
      Thread.sleep(1000);
  }

  private boolean isIndexingFinished(long nbDocs) throws Exception {
    HttpClientConfig.Builder builder =
        new HttpClientConfig.Builder("http://" + ES_IP + ":" + ES_HTTP_PORT).multiThreaded(true);
    JestClientFactory factory = new JestClientFactory();
    factory.setHttpClientConfig(builder.build());
    JestClient client = factory.getObject();

    Stats stats = new Stats.Builder().addIndex(ES_INDEX).setParameter("level", "indices").build();
    JestResult result = client.execute(stats);
    client.shutdownClient();
    long count = 0;
    if (result.isSucceeded()) {
      JsonObject jsonObject = result.getJsonObject();
      JsonObject docs =
          jsonObject.getAsJsonObject("indices").getAsJsonObject(ES_INDEX).getAsJsonObject
              ("total").getAsJsonObject("docs");
      count = docs.getAsJsonPrimitive("count").getAsLong();
    }
    return count == nbDocs;
  }

  @Test
  @Category(NeedsRunner.class)
  public void testRead() throws Exception {
    sampleIndex(NB_DOCS);
    String[] args = new String[] { "--runner=FlinkRunner", "--project=test-project" };

    TestPipeline pipeline =
        TestPipeline.fromOptions(PipelineOptionsFactory.fromArgs(args).create());

    PCollection<String> output = pipeline.apply(
        ElasticsearchIO.read().withAddress("http://" + ES_IP + ":" + ES_HTTP_PORT).withIndex(
            ES_INDEX).withType(ES_TYPE));
    PAssert.thatSingleton(output.apply("Count", Count.<String>globally())).isEqualTo(NB_DOCS);
    output.apply(ParDo.of(new DoFn<String, String>() {
      @ProcessElement
      public void processElement(ProcessContext context) {
        String element = context.element();
        LOGGER.info(element);
      }
    }));
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testReadWithQuery() throws Exception {
    sampleIndex(NB_DOCS);

    String query = "{\n"
        + "  \"query\": {\n"
        + "  \"match\" : {\n"
        + "    \"scientist\" : {\n"
        + "      \"query\" : \"Einstein\",\n"
        + "      \"type\" : \"boolean\"\n"
        + "    }\n"
        + "  }\n"
        + "  }\n"
        + "}";

    Pipeline pipeline = TestPipeline.create();

    PCollection<String> output = pipeline.apply(
        ElasticsearchIO.read().withAddress("http://" + ES_IP + ":" + ES_HTTP_PORT).withQuery(
            query).withIndex(ES_INDEX).withType(ES_TYPE));
    PAssert.thatSingleton(output.apply("Count", Count.<String>globally())).isEqualTo(NB_DOCS / 10);
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWrite() throws Exception {
    Pipeline pipeline = TestPipeline.create();

    String[] scientists =
        { "Einstein", "Darwin", "Copernicus", "Pasteur", "Curie", "Faraday", "Newton", "Bohr",
            "Galilei", "Maxwell" };
    ArrayList<String> data = new ArrayList<>();
    for (int i = 0; i < NB_DOCS; i++) {
      int index = i % scientists.length;
      data.add(String.format("{\"scientist\":\"%s\", \"id\":%d}", scientists[index], i));
    }
    pipeline.apply(Create.of(data)).apply(
        ElasticsearchIO.write().withAddress("http://" + ES_IP + ":" + ES_HTTP_PORT).withIndex(
            ES_INDEX).withType(ES_TYPE));

    pipeline.run();

    waitForESIndexationToFinish();
    SearchResponse response = node.client().prepareSearch().execute().actionGet(5000);
    Assert.assertEquals(NB_DOCS, response.getHits().getTotalHits());

    QueryBuilder queryBuilder = QueryBuilders.queryStringQuery("Einstein").field("scientist");
    response = node.client().prepareSearch().setQuery(queryBuilder).execute().actionGet();
    Assert.assertEquals(NB_DOCS / 10, response.getHits().getTotalHits());
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWriteWithBatchSizes() throws Exception {
    Pipeline pipeline = TestPipeline.create();

    String[] scientists =
        { "Einstein", "Darwin", "Copernicus", "Pasteur", "Curie", "Faraday", "Newton", "Bohr",
            "Galilei", "Maxwell" };
    ArrayList<String> data = new ArrayList<>();
    for (int i = 0; i < NB_DOCS; i++) {
      int index = i % scientists.length;
      data.add(String.format("{\"scientist\":\"%s\", \"id\":%d}", scientists[index], i));
    }
    PDone collection = pipeline.apply(Create.of(data)).apply(
        ElasticsearchIO.write().withAddress("http://" + ES_IP + ":" + ES_HTTP_PORT).withIndex(
            ES_INDEX).withType(ES_TYPE).withBatchSize(NB_DOCS / 2).withBatchSizeMegaBytes(1));

    //TODO assert nb bundles == 2
    pipeline.run();
    waitForESIndexationToFinish();

    SearchResponse response = node.client().prepareSearch().execute().actionGet(5000);
    Assert.assertEquals(NB_DOCS, response.getHits().getTotalHits());

    QueryBuilder queryBuilder = QueryBuilders.queryStringQuery("Einstein").field("scientist");
    response = node.client().prepareSearch().setQuery(queryBuilder).execute().actionGet();
    Assert.assertEquals(NB_DOCS / 10, response.getHits().getTotalHits());
  }

  @Test
  public void testSplitsWithDesiredBundleSizeBiggerThanShardSize() throws Exception {
    sampleIndex(NB_DOCS);
    PipelineOptions options = PipelineOptionsFactory.create();
    ElasticsearchIO.Read read =
        ElasticsearchIO.read().withAddress("http://" + ES_IP + ":" + ES_HTTP_PORT).withIndex(
            ES_INDEX).withType(ES_TYPE).withScrollKeepalive("10m");
    BoundedElasticsearchSource initialSource = read.getSource();
    //ES creates 5 shards for that amount of data, so there should be 5 split because bundlesize
    // is > to shard size
    int desiredBundleSizeBytes = 1073741824;
    List<? extends BoundedSource<String>> splits = initialSource.splitIntoBundles(
        desiredBundleSizeBytes, options);
    SourceTestUtils.
        assertSourcesEqualReferenceSource(initialSource, splits, options);
    int expectedNbSplits = 5;
    assertEquals(expectedNbSplits, splits.size());
    int nonEmptySplits = 0;
    for (BoundedSource<String> subSource : splits)
      if (readFromSource(subSource, options).size() > 0) {
        nonEmptySplits += 1;
      }
    assertEquals(expectedNbSplits, nonEmptySplits);

  }

  @Test
  public void testSplitsWithSmallerDesiredBundleSizeThanShardSize() throws Exception {
    //put all docs in one shard
    long dataSize = sampleIndex(NB_DOCS);
    PipelineOptions options = PipelineOptionsFactory.create();
    ElasticsearchIO.Read read =
        ElasticsearchIO.read().withAddress("http://" + ES_IP + ":" + ES_HTTP_PORT).withIndex(
            ES_INDEX).withType(ES_TYPE).withScrollKeepalive("10m");
    BoundedElasticsearchSource initialSource = read.getSource();
    long desiredBundleSizeBytes = 60;
    List<? extends BoundedSource<String>> splits = initialSource.splitIntoBundles(
        desiredBundleSizeBytes, options);
    SourceTestUtils.
        assertSourcesEqualReferenceSource(initialSource, splits, options);
    long expectedNbSplits = 9;
    assertEquals(expectedNbSplits, splits.size());
    int nonEmptySplits = 0;
    for (BoundedSource<String> subSource : splits)
        if (readFromSource(subSource, options).size() > 0) {
        nonEmptySplits += 1;
      }
    assertEquals(3, nonEmptySplits);

  }

  @After
  public void after() throws Exception {
    if (node != null) {
      node.close();
    }
  }

}
