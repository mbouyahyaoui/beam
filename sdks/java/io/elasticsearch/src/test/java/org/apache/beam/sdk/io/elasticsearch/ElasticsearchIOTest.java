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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.commons.io.FileUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
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
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;

/**
 * Test on {@link ElasticsearchIO}.
 */
public class ElasticsearchIOTest implements Serializable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchIOTest.class);
  private static final String DATA_DIRECTORY = "target/elasticsearch";

  private transient Node node;

  @Before
  public void before() throws Exception {
    LOGGER.info("Starting embedded Elasticsearch instance");
    File file = new File(DATA_DIRECTORY);
    if (file.exists()) {
      file.delete();
    }
    Settings.Builder settingsBuilder = Settings.settingsBuilder()
        .put("cluster.name", "beam")
        .put("http.enabled", "true")
        .put("node.data", "true")
        .put("path.data", DATA_DIRECTORY)
        .put("path.home", DATA_DIRECTORY)
        .put("node.name", "beam")
        .put("network.host", "localhost")
        .put("port", 9301)
        .put("http.port", 9201);
    node = NodeBuilder.nodeBuilder().settings(settingsBuilder).build();
    LOGGER.info("Elasticsearch node created");
    if (node != null) {
      node.start();
    }
  }

  private void sampleIndex() throws Exception {
    String[] scientists = {"Einstein", "Darwin", "Copernicus", "Pasteur", "Curie", "Faraday",
        "Newton", "Bohr", "Galilei", "Maxwell"};
    for (int i = 0; i < 100; i++) {
      int index = i % scientists.length;
      String source = String.format("{\"scientist\":\"%s\", \"id\":%d}", scientists[index], i);
      node.client().prepareIndex("beam", "test").setSource(source).get();
    }
  }

  @Test
  @Category(NeedsRunner.class)
  public void testFullRead() throws Exception {
    sampleIndex();

    Pipeline pipeline = TestPipeline.create();

    PCollection<String> output =
        pipeline.apply(ElasticsearchIO.read().withAddress("http://localhost:9201"));
    PAssert.thatSingleton(output.apply("Count", Count.<String>globally())).isEqualTo(100L);
    output.apply(ParDo.of(new DoFn<String, String>() {
      @ProcessElement
      public void processElement(ProcessContext context) {
        String element = context.element();
        System.out.println(element);
      }
    }));
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testIndexRead() throws Exception {
    sampleIndex();

    Pipeline pipeline = TestPipeline.create();

    PCollection<String> output =
        pipeline.apply(ElasticsearchIO.read().withAddress("http://localhost:9201")
            .withIndex("beam"));
    PAssert.thatSingleton(output.apply("Count", Count.<String>globally())).isEqualTo(100L);
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testQueryRead() throws Exception {
    sampleIndex();

    QueryBuilder qb = QueryBuilders.matchQuery("scientist", "Einstein");

    Pipeline pipeline = TestPipeline.create();

    PCollection<String> output =
        pipeline.apply(ElasticsearchIO.read().withAddress("http://localhost:9201")
            .withQuery(qb.toString()));
    PAssert.thatSingleton(output.apply("Count", Count.<String>globally())).isEqualTo(10L);
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWrite() throws Exception {
    Pipeline pipeline = TestPipeline.create();

    String[] scientists = {"Einstein", "Darwin", "Copernicus", "Pasteur", "Curie", "Faraday",
        "Newton", "Bohr", "Galilei", "Maxwell"};
    ArrayList<String> data = new ArrayList<>();
    for (int i = 0; i < 2000; i++) {
      int index = i % scientists.length;
      data.add(String.format("{\"scientist\":\"%s\", \"id\":%d}", scientists[index], i));
    }
    pipeline.apply(Create.of(data))
        .apply(ElasticsearchIO.write().withAddress("http://localhost:9201")
            .withIndex("beam").withType("test"));

    pipeline.run();

    // gives time for bulk to complete
    Thread.sleep(5000);

    SearchResponse response = node.client().prepareSearch().execute().actionGet(5000);
    Assert.assertEquals(2000, response.getHits().getTotalHits());

    QueryBuilder queryBuilder = QueryBuilders.queryStringQuery("Einstein").field("scientist");
    response = node.client().prepareSearch().setQuery(queryBuilder)
        .execute().actionGet();
    Assert.assertEquals(200, response.getHits().getTotalHits());
  }

    @Test
    @Category(NeedsRunner.class)
    public void testWriteWithBatchSizes() throws Exception {
        Pipeline pipeline = TestPipeline.create();

        String[] scientists = {"Einstein", "Darwin", "Copernicus", "Pasteur", "Curie", "Faraday",
                "Newton", "Bohr", "Galilei", "Maxwell"};
        ArrayList<String> data = new ArrayList<>();
        for (int i = 0; i < 2000; i++) {
            int index = i % scientists.length;
            data.add(String.format("{\"scientist\":\"%s\", \"id\":%d}", scientists[index], i));
        }
        PDone collection = pipeline.apply(Create.of(data)).apply(
                ElasticsearchIO.write().withAddress("http://localhost:9201").withIndex("beam")
                        .withType("test").withBatchSize(2000).withBatchSizeMegaBytes(1));

        //TODO assert nb bundles == 1
        pipeline.run();

        // gives time for bulk to complete
        Thread.sleep(5000);

        SearchResponse response = node.client().prepareSearch().execute().actionGet(5000);
        Assert.assertEquals(2000, response.getHits().getTotalHits());

        QueryBuilder queryBuilder = QueryBuilders.queryStringQuery("Einstein").field("scientist");
        response = node.client().prepareSearch().setQuery(queryBuilder)
                .execute().actionGet();
        Assert.assertEquals(200, response.getHits().getTotalHits());
    }

  @After
  public void after() throws Exception {
    if (node != null) {
      node.close();
    }
    cleanEmbededElasticSearch();
  }

  private void cleanEmbededElasticSearch() {
    try {
        FileUtils.deleteDirectory(new File(DATA_DIRECTORY));
    } catch (IOException e) {
        throw new RuntimeException("Could not delete data directory of embedded elasticsearch server", e);
    }
  }
}
