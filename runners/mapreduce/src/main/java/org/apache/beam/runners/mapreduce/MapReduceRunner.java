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
package org.apache.beam.runners.mapreduce;

import org.apache.beam.runners.mapreduce.translation.MapReducePipelineTranslator;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.hadoop.mapreduce.Job;

/**
 * A {@link PipelineRunner} that translates a pipeline to a MapReduce job.
 */
public class MapReduceRunner extends PipelineRunner<MapReduceRunnerResult> {

  private final MapReducePipelineOptions options;

  public MapReduceRunner(MapReducePipelineOptions options) {
    this.options = options;
  }

  @Override
  public MapReduceRunnerResult run(Pipeline pipeline) {
    MapReducePipelineTranslator translator = new MapReducePipelineTranslator(options);
    try {
      Job job = Job.getInstance(null, "NAME");
      translator.translate(pipeline, job);
      MapReduceRunnerResult result = new MapReduceRunnerResult(job);
      return result;
    } catch (Exception e) {
      throw new RuntimeException("Can't run job", e);
    }
  }

}
