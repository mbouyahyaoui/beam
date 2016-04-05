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

import com.google.cloud.dataflow.sdk.PipelineResult;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsValidator;
import com.google.cloud.dataflow.sdk.runners.PipelineRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The MapReducePipelineRunner translates operations defined on a pipeline to a representation
 * executable by MapReduce, and then submitting the job to MapReduce to be executed.
 *
 * Note: by design, the MapReducePipelineRunner is only to deal with batch mode (not stream).
 *
 * To create a pipeline runner to run against on a MapReduce resource manager, with a master
 * URL:
 *
 * {@code
 *  Pipeline p = [logic for pipeline creation]
 *  MapReducePipelineOptions options = MapReducePipelineOptionsFactory.create();
 *  options.setMaster("");
 *  EvaluationResult result = MapReducPipelineRunner.create(options).run(p);
 * }
 */
public final class MapReducePipelineRunner extends PipelineRunner<MapReduceRunnerResult> {

  private static final Logger LOG = LoggerFactory.getLogger(MapReducePipelineRunner.class);

  /**
   * Options used in this pipeline runner.
   */
  private final MapReducePipelineOptions options;

  public static MapReducePipelineRunner create() {
    MapReducePipelineOptions options = MapReducePipelineOptionsFactory.create();
    return new MapReducePipelineRunner(options);
  }

  public static MapReducePipelineRunner create(MapReducePipelineOptions options) {
    return new MapReducePipelineRunner(options);
  }

  public static MapReducePipelineRunner fromOptions(PipelineOptions options) {
    MapReducePipelineOptions mrOptions = PipelineOptionsValidator.validate(MapReducePipelineOptions.class, options);
    return new MapReducePipelineRunner(mrOptions);
  }

  private MapReducePipelineRunner(MapReducePipelineOptions options) {
    this.options = options;
  }



}
