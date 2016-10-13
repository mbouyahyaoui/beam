package org.apache.beam.sdk.io.elasticsearch;

import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIOTest.ES_INDEX;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIOTest.ES_TYPE;
import static org.apache.beam.sdk.testing.SourceTestUtils.readFromSource;
import static org.junit.Assert.assertEquals;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.core.Bulk;
import io.searchbox.core.BulkResult;
import io.searchbox.core.Index;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.Flush;
import io.searchbox.indices.Stats;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.SourceTestUtils;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.values.PCollection;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.flush.SyncedFlushRequest;
import org.elasticsearch.action.admin.indices.upgrade.post.UpgradeRequest;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.indexing.IndexingStats;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Integration test for Elasticsearch IO.
 */
public class ElasticsearchIOIT {
  private static final String ES_IP = "localhost";
  private static final String ES_HTTP_PORT = "9200";

  //ignored for now because needs ES integration test environment
  @Ignore
  @Test
  public void testSplitsVolume() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    ElasticsearchIO.Read read =
        ElasticsearchIO.read().withAddress("http://" + ES_IP + ":" + ES_HTTP_PORT).withIndex(
            ES_INDEX).withType(ES_TYPE);
    ElasticsearchIO.BoundedElasticsearchSource initialSource = read.getSource();
    long desiredBundleSizeBytes = 4000;
    List<? extends BoundedSource<String>> splits = initialSource.splitIntoBundles(
        desiredBundleSizeBytes, options);
    SourceTestUtils.
        assertSourcesEqualReferenceSource(initialSource, splits, options);
    long expectedNbSplits = 15;
    assertEquals(expectedNbSplits, splits.size());
    int nonEmptySplits = 0;
    for (BoundedSource<String> subSource : splits) {
      if (readFromSource(subSource, options).size() > 0) {
        nonEmptySplits += 1;
      }
    }
    assertEquals(expectedNbSplits, nonEmptySplits);
  }

  //ignored for now because needs ES integration test environment
  @Ignore
  @Test
  @Category(NeedsRunner.class)
  public void testReadVolume() throws Exception {
    String[] args = new String[] { "--runner=FlinkRunner", "--project=test-project" };

    TestPipeline pipeline =
        TestPipeline.fromOptions(PipelineOptionsFactory.fromArgs(args).create());

    PCollection<String> output = pipeline.apply(
        ElasticsearchIO.read().withAddress("http://" + ES_IP + ":" + ES_HTTP_PORT).withIndex(
            ES_INDEX).withType(ES_TYPE));
    PAssert.thatSingleton(output.apply("Count", Count.<String>globally())).isEqualTo(1000L);
    pipeline.run();
  }

  //ignored for now because needs ES integration test environment
  @Ignore
  @Test
  public void testEstimatedSizesVolume() throws IOException {
    PipelineOptions options = PipelineOptionsFactory.create();
    ElasticsearchIO.Read read =
        ElasticsearchIO.read().withAddress("http://" + ES_IP + ":" + ES_HTTP_PORT).withIndex(
            ES_INDEX).withType(ES_TYPE);
    ElasticsearchIO.BoundedElasticsearchSource initialSource = read.getSource();
    assertEquals("Wrong estimated size", 1928649, initialSource.getEstimatedSizeBytes
        (options));
    assertEquals("Wrong average doc size", 27, initialSource.getAverageDocSize());
  }

  @Test
  public void testESBulkInsert() throws Exception {
    Settings settings = Settings.settingsBuilder()
        .put("cluster.name", "beam")
        .build();
    InetSocketTransportAddress address =
        new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300);
    Client client = TransportClient.builder().settings(settings).build()
        .addTransportAddress(address);

    BulkProcessor bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
      @Override
      public void beforeBulk(long executionId, BulkRequest request) {

      }

      @Override
      public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {

      }

      @Override
      public void afterBulk(long executionId, BulkRequest request, Throwable failure) {

      }
    })
        .setFlushInterval(TimeValue.timeValueSeconds(10))
        .setBulkActions(1000)
        .build();
    for (int i = 0; i < 10000; i++) {
      IndexRequest indexRequest = new IndexRequest("scientist", "default")
          .source("{ \"name\" : \"Einstein\" }");
      bulkProcessor.add(indexRequest);
    }

    // clearing cache
    ClearIndicesCacheRequest clearCacheRequest = new ClearIndicesCacheRequest("scientist");
    client.admin().indices().clearCache(clearCacheRequest).actionGet();

    // flushing
    FlushRequest flushRequest = new FlushRequest("scientist").force(true).waitIfOngoing(true);
    client.admin().indices().flush(flushRequest).actionGet();

    // flush sync
    SyncedFlushRequest syncedFlushRequest = new SyncedFlushRequest();
    client.admin().indices().syncedFlush(syncedFlushRequest).actionGet();

    // upgrade
    UpgradeRequest upgradeRequest = new UpgradeRequest();
    client.admin().indices().upgrade(upgradeRequest).actionGet();
  }

  @Test
  public void testESSize() throws Exception {

  }

  @Test
  public void testJestBulkInsert() throws Exception {
    JestClientFactory factory = new JestClientFactory();
    JestClient client = factory.getObject();
    for (int i = 0; i < 100; i++) {
      ArrayList<Index> batch = new ArrayList();
      for (int j = 0; j < 100; j++) {
        batch.add(new Index.Builder("{ \"name\" : \"Einstein\" }").index("scientist").type
            ("default")
            .build());
      }
      Bulk bulk = new Bulk.Builder()
          .defaultIndex("scientist")
          .defaultType("default")
          .addAction(batch)
          .build();
      BulkResult result = client.execute(bulk);
      if (!result.isSucceeded()) {
        System.out.println("Bulk failed");
        System.out.println(result.getErrorMessage());
      }
      batch.clear();
    }

    Flush flush = new Flush.Builder().setParameter("wait_for_ongoing", null).build();
    JestResult flushResult = client.execute(flush);
    if (!flushResult.isSucceeded()) {
      System.out.println(flushResult.getErrorMessage());
    }
    System.out.println(flushResult.getJsonString());
  }

  @Test
  public void testJestSize() throws Exception {
    JestClientFactory factory = new JestClientFactory();
    JestClient client = factory.getObject();
    System.out.println("Stats");
    Stats stats = new Stats.Builder().addIndex("scientist").setParameter("level", "shards").build();
    JestResult result = client.execute(stats);
    if (result.isSucceeded()) {
      JsonObject jsonObject = result.getJsonObject();
      long indexSize = jsonObject.getAsJsonObject("indices").getAsJsonObject("scientist")
          .getAsJsonObject("total").getAsJsonObject("store").getAsJsonPrimitive("size_in_bytes")
          .getAsLong();
      System.out.println("Index: " + indexSize);
      JsonObject shardsJson = jsonObject.getAsJsonObject("indices").getAsJsonObject("scientist")
          .getAsJsonObject("shards");
      Set<Map.Entry<String, JsonElement>> entries = shardsJson.entrySet();
      for (Map.Entry<String, JsonElement> shardJson : entries) {
        String shardId = shardJson.getKey();
        JsonArray value = (JsonArray) shardJson.getValue();
        long shardSize =
            value.get(0).getAsJsonObject().getAsJsonObject("store").getAsJsonPrimitive(
                "size_in_bytes").getAsLong();
        System.out.println("Shard " + shardId + ": " + shardSize);
      }
    } else {
      System.out.println("Stats failed");
      System.out.println(result.getErrorMessage());
    }
  }

}
