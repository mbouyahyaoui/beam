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

import java.io.IOException;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.joda.time.Duration;

public class MapReduceRunnerResult implements PipelineResult {

  private final Job job;
  private State state = State.UNKNOWN;

  public MapReduceRunnerResult(Job job) {
    this.job = job;
  }

  @Override
  public State getState() {
    try {
      ControlledJob.State currentState = job.getJobState():
      if (currentState == ControlledJob.State.FAILED
          || currentState == ControlledJob.State.DEPENDENT_FAILED) {
        state = State.FAILED;
      }
      if (currentState == ControlledJob.State.RUNNING) {
        state = State.RUNNING;
      }
      if (currentState == ControlledJob.State.SUCCESS) {
        state = State.DONE;
      }
    } catch (Exception e) {
      // nothing to do
    }
    return state;
  }

  @Override
  public State cancel() throws IOException {
    job.killJob();
    state = State.CANCELLED;
    return state;
  }

  // TODO add timeout support on execution/duration
  @Override
  public State waitUntilFinish(Duration duration) {
    try {
      job.waitForCompletion(true);
    } catch (Exception e) {
      // nothing to do
    }
    state = State.DONE;
    return state;
  }

  @Override
  public State waitUntilFinish() {
    return waitUntilFinish(Duration.ZERO);
  }

  @Override
  public MetricResults metrics() {
    return null;
  }
}
