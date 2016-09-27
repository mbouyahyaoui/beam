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

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import com.google.common.base.Throwables;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.NoopCredentialFactory;
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

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

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
    private final AtomicLong pendingBulkItemCount = new AtomicLong();
    private final int concurrentRequests;

    public BulkListener(int concurrentRequests) {
      this.concurrentRequests = concurrentRequests;
    }

    @Override
    public void beforeBulk(long executionId, BulkRequest request) {
      pendingBulkItemCount.addAndGet(request.numberOfActions());
    }

    @Override
    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
      LOGGER.warn("Can't append into Elasticsearch", failure);
      pendingBulkItemCount.addAndGet(-request.numberOfActions());
    }

    @Override
    public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
      pendingBulkItemCount.addAndGet(-response.getItems().length);
    }

    public void waitFinished() {
      while (concurrentRequests > 0 && pendingBulkItemCount.get() > 0) {
        LockSupport.parkNanos(1000 * 50);
      }
    }
  }

  private void sampleIndex() throws Exception {
    BulkProcessor bulkProcessor =
        BulkProcessor.builder(node.client(), new BulkListener(5))
            .setBulkActions(100)
            .setConcurrentRequests(5)
            .setFlushInterval(TimeValue.timeValueSeconds(5))
            .build();
    String[] scientists =
        { "Einstein", "Darwin", "Copernicus", "Pasteur", "Curie", "Faraday", "Newton", "Bohr",
            "Galilei", "Maxwell" };
    for (int i = 0; i < 1000; i++) {
      int index = i % scientists.length;
      String source = String.format("{\"scientist\":\"%s\", \"id\":%d}", scientists[index], i);
      bulkProcessor.add(new IndexRequest(ES_INDEX, ES_TYPE).source(source));
    }
    bulkProcessor.close();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testRead() throws Exception {
    sampleIndex();
    String[] args = new String[] { "--runner=FlinkRunner", "--project=test-project"};

    TestPipeline pipeline =
        TestPipeline.fromOptions(PipelineOptionsFactory.fromArgs(args).create());

    PCollection<String> output = pipeline.apply(
        ElasticsearchIO.read().withAddress("http://" + ES_IP + ":" + ES_HTTP_PORT).withIndex(
            ES_INDEX).withType(ES_TYPE));
    PAssert.thatSingleton(output.apply("Count", Count.<String>globally())).isEqualTo(1000L);
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
    sampleIndex();

    QueryBuilder qb = QueryBuilders.matchQuery("scientist", "Einstein");

    Pipeline pipeline = TestPipeline.create();

    PCollection<String> output = pipeline.apply(
        ElasticsearchIO.read().withAddress("http://" + ES_IP + ":" + ES_HTTP_PORT).withQuery(
            qb.toString()).withIndex(ES_INDEX).withType(ES_TYPE));
    PAssert.thatSingleton(output.apply("Count", Count.<String>globally())).isEqualTo(100L);
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
    for (int i = 0; i < 2000; i++) {
      int index = i % scientists.length;
      data.add(String.format("{\"scientist\":\"%s\", \"id\":%d}", scientists[index], i));
    }
    pipeline.apply(Create.of(data)).apply(
        ElasticsearchIO.write().withAddress("http://" + ES_IP + ":" + ES_HTTP_PORT).withIndex(
            ES_INDEX).withType(ES_TYPE));

    pipeline.run();

    // gives time for bulk to complete
    Thread.sleep(5000);

    SearchResponse response = node.client().prepareSearch().execute().actionGet(5000);
    Assert.assertEquals(2000, response.getHits().getTotalHits());

    QueryBuilder queryBuilder = QueryBuilders.queryStringQuery("Einstein").field("scientist");
    response = node.client().prepareSearch().setQuery(queryBuilder).execute().actionGet();
    Assert.assertEquals(200, response.getHits().getTotalHits());
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWriteWithBatchSizes() throws Exception {
    Pipeline pipeline = TestPipeline.create();

    String[] scientists =
        { "Einstein", "Darwin", "Copernicus", "Pasteur", "Curie", "Faraday", "Newton", "Bohr",
            "Galilei", "Maxwell" };
    ArrayList<String> data = new ArrayList<>();
    for (int i = 0; i < 2000; i++) {
      int index = i % scientists.length;
      data.add(String.format("{\"scientist\":\"%s\", \"id\":%d}", scientists[index], i));
    }
    PDone collection = pipeline.apply(Create.of(data)).apply(
        ElasticsearchIO.write().withAddress("http://" + ES_IP + ":" + ES_HTTP_PORT).withIndex(
            ES_INDEX).withType(ES_TYPE).withBatchSize(2000).withBatchSizeMegaBytes(1));

    //TODO assert nb bundles == 1
    pipeline.run();

    // gives time for bulk to complete
    Thread.sleep(5000);

    SearchResponse response = node.client().prepareSearch().execute().actionGet(5000);
    Assert.assertEquals(2000, response.getHits().getTotalHits());

    QueryBuilder queryBuilder = QueryBuilders.queryStringQuery("Einstein").field("scientist");
    response = node.client().prepareSearch().setQuery(queryBuilder).execute().actionGet();
    Assert.assertEquals(200, response.getHits().getTotalHits());
  }

  @After
  public void after() throws Exception {
    if (node != null) {
      node.close();
    }
  }

}
