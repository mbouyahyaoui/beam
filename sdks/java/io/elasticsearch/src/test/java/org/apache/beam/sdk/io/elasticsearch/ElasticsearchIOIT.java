package org.apache.beam.sdk.io.elasticsearch;

import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.SourceTestUtils;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.List;

import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIOTest.ES_INDEX;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIOTest.ES_TYPE;
import static org.junit.Assert.assertEquals;

public class ElasticsearchIOIT {
  private static final String ES_IP = "localhost";
  private static final String ES_HTTP_PORT = "9200";

  //ignored for now because needs ES integration test environment
  @Ignore
  @Test
  @Category(NeedsRunner.class)
  public void testRead() throws Exception {
    String[] args = new String[] { "--runner=FlinkRunner", "--project=test-project" };

    TestPipeline pipeline =
        TestPipeline.fromOptions(PipelineOptionsFactory.fromArgs(args).create());

    PCollection<String> output = pipeline.apply(
        ElasticsearchIO.read().withAddress("http://" + ES_IP + ":" + ES_HTTP_PORT).withIndex(
            ES_INDEX).withType(ES_TYPE));
    PAssert.thatSingleton(output.apply("Count", Count.<String>globally())).isEqualTo(10L);
    pipeline.run();
  }

  //ignored for now because needs ES integration test environment
  @Ignore
  @Test
  public void testVolumeEstimatedSizes() throws IOException {
    PipelineOptions options = PipelineOptionsFactory.create();
    ElasticsearchIO.Read read =
        ElasticsearchIO.read().withAddress("http://" + ES_IP + ":" + ES_HTTP_PORT).withIndex(
            ES_INDEX).withType(ES_TYPE);
    ElasticsearchIO.BoundedElasticsearchSource initialSource = read.getSource();
    assertEquals("Wrong estimated size", 1928649, initialSource.getEstimatedSizeBytes
        (options));
    assertEquals("Wrong average doc size", 27, initialSource.getAverageDocSize());

  }

}
