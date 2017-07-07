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
package org.apache.beam.sdk.io.influxdb;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * TODO: describe the usage of the IO.
 */
public class InfluxDbIO {

  private static final Logger LOG = LoggerFactory.getLogger(InfluxDbIO.class);

  /**
   * Interface for the parser that is used to parse the GridFSDBFile into
   * the appropriate types.
   * @param <T>
   */
  public interface Parser<T> extends Serializable {
    T parse(String input) throws IOException;
  }

  /**
   * For the default {@code Read<String>} case, this is the parser that is used to
   * split the input file into Strings. It uses the timestamp of the file
   * for the event timestamp.
   */
  private static final Parser<String> TEXT_PARSER = new Parser<String>() {
    @Override
    public String parse(String input) throws IOException {
      return input;
    }
  };

  /**
   * Read data from a InfluxDb.
   *
   * @param <T> Type of the data to be read.
   */
  public static <T> Read<T> read() {
    return new AutoValue_InfluxDbIO_Read.Builder<T>().build();
  }

  /**
   * A {@link PTransform} to read data from InfluxDB.
   */
  @AutoValue
  public abstract static class Read<T> extends PTransform<PBegin, PCollection<T>> {

    @Nullable abstract Parser<T> parser();
    @Nullable abstract String uri();
    @Nullable abstract String database();
    @Nullable abstract Coder<T> coder();
    abstract Builder<T> builder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setParser(Parser<T> parser);
      abstract Builder<T> setUri(String uri);
      abstract Builder<T> setDatabase(String database);
      abstract Builder<T> setCoder(Coder<T> coder);
      abstract Read<T> build();
    }

    public Read<T> withParser(Parser<T> parser) {
      checkArgument(parser != null, "InfluxDbIO.read().withParser(parser) called with null "
          + "parser");
      return builder().setParser(parser).build();
    }

    public Read<T> withUri(String uri) {
      checkArgument(uri != null, "InfluxDbIO.read().withUri(uri) called with null uri");
      return builder().setUri(uri).build();
    }

    public Read<T> withDatabase(String database) {
      checkArgument(database != null, "InfluxDbIO.read().withDatabase(database) called with "
          + "null database");
      return builder().setDatabase(database).build();
    }

    public Read<T> withCoder(Coder<T> coder) {
      checkArgument(coder != null, "InfluxDbIO.read().withCoder(coder) called with null coder");
      return builder().setCoder(coder).build();
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      return input.getPipeline()
          .apply(org.apache.beam.sdk.io.Read.from(new InfluxDbSource<T>(this)));
    }

    @Override
    public void validate(PipelineOptions pipelineOptions) {
      checkState(uri() != null, "InfluxDbIO.read() requires an uri to be set via withUri"
          + "(uri)");
      checkState(database() != null, "InfluxDbIO.read() requires a database to be set via "
          + "withDatabase(database)");
      checkState(coder() != null, "InfluxDbIO.read() requires a coder to be set via "
          + "withCoder(coder)");
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("uri", uri()));
      builder.add(DisplayData.item("database", database()));
      builder.add(DisplayData.item("coder", coder().getClass().getName()));
    }

  }

  private static class InfluxDbSource<T> extends BoundedSource<T> {

    private Read spec;

    public InfluxDbSource(Read spec) {
      this.spec = spec;
    }

    @Override
    public Coder<T> getDefaultOutputCoder() {
      return spec.coder();
    }

    @Override
    public void validate() {
      spec.validate(null);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      spec.populateDisplayData(builder);
    }

    @Override
    public BoundedReader<T> createReader(PipelineOptions options) throws IOException {
      return new InfluxDbReader(this);
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
      // TODO add logging on interesting stuff like the query and query result)
      // TODO reuse the same connection between split and estimated size
      LOG.debug("Estimating the InfluxDB size");
      InfluxDB influxDB = InfluxDBFactory.connect(spec.uri());
      influxDB.createDatabase(spec.database());
      Query query = new Query("SELECT * FROM " + spec.database(), spec.database());
      QueryResult queryResult = influxDB.query(query);
      List databaseNames = queryResult.getResults().get(0).getSeries().get(0).getValues();
      int size = 0;
      if(databaseNames != null) {
        Iterator var4 = databaseNames.iterator();
        while(var4.hasNext()) {
          List database = (List)var4.next();
          size += database.size();
        }
      }
      return size;
    }

    @Override
    public List<? extends BoundedSource<T>> split(long desiredBundleSizeBytes,
                                                       PipelineOptions options) throws Exception {
      List<BoundedSource<T>> sources = new ArrayList<>();
      InfluxDB influxDB = InfluxDBFactory.connect(spec.uri());
      influxDB.createDatabase(spec.database());
      Query query = new Query("SELECT * FROM " + spec.database(), spec.database());
      QueryResult queryResult = influxDB.query(query);
      List databaseNames = queryResult.getResults().get(0).getSeries().get(0).getValues();
      int size = 0;
      if(databaseNames != null) {
        Iterator var4 = databaseNames.iterator();
        while(var4.hasNext()) {
          List database = (List)var4.next();
          sources.add(this);// Need to check .. Rama
        }
      }
      checkArgument(!sources.isEmpty(), "No primary shard found");
      return sources;
    }
  }

  // TODO the reader can be a BoundedReader<T> and use the parser for each element read, largely
  // more efficient !
  private static class InfluxDbReader<T> extends BoundedSource.BoundedReader<T> {

    private final InfluxDbSource<T> source;
    private Iterator cursor;
    private T current;
    private InfluxDB influxDB;

    public InfluxDbReader(InfluxDbSource<T> source) {
      this.source = source;
    }

    @Override
    public boolean start() throws IOException {
      Read spec = source.spec;
      LOG.debug("Starting reading from InfluxDB {} at {}", spec.database(), spec.uri());
      influxDB = InfluxDBFactory.connect(spec.uri());
      influxDB.createDatabase(spec.database());
      Query query = new Query("SELECT * FROM " + spec.database(), spec.database());
      QueryResult queryResult = influxDB.query(query);
      List databaseNames = queryResult.getResults().get(0).getSeries().get(0).getValues();
      if(databaseNames != null) {
        cursor = databaseNames.iterator();
      }
      return advance();
    }

    @Override
    public boolean advance() throws IOException {
      if (cursor.hasNext()) {
        String element = cursor.next().toString();
        current = (T) source.spec.parser().parse(element);
        return true;
      } else {
        return false;
      }
    }

    @Override
    public BoundedSource<T> getCurrentSource() {
      return source;
    }

    @Override
    public T getCurrent() throws NoSuchElementException {
      if (current == null) {
        throw new NoSuchElementException();
      }
      return current;
    }

    @Override
    public void close() throws IOException {
      return;
      // TODO
//            try {
//                if (cursor != null) {
//                    cursor.close();
//                }
//            } catch (Exception e) {
//                LOG.warn("Error closing MongoDB cursor", e);
//            }
//            try {
//                client.close();
//            } catch (Exception e) {
//                LOG.warn("Error closing MongoDB client", e);
//            }
    }
  }

  /** Write data to InfluxDB. */
  public static Write write() {
    return new AutoValue_InfluxDbIO_Write.Builder().setBatchSize(1024).build();
  }

  /**
   * A {@link PTransform} to write to a InfluxDB database.
   * // TODO use parser on the incoming PCollection element and use T for the writer
   */
  @AutoValue
  public abstract static class Write extends PTransform<PCollection<String>, PDone> {

    @Nullable abstract String uri();
    @Nullable abstract String database();
    abstract long batchSize();
    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setUri(String uri);
      abstract Builder setDatabase(String database);
      abstract Builder setBatchSize(long batchSize);
      abstract Write build();
    }

    public Write withUri(String uri) {
      // TODO add check
      return builder().setUri(uri).build();
    }

    public Write withDatabase(String database) {
      // TODO add check
      return builder().setDatabase(database).build();
    }

    public Write withBatchSize(long batchSize) {
      // TODO add check
      return builder().setBatchSize(batchSize).build();
    }

    @Override
    public PDone expand(PCollection<String> input) {
      input.apply(ParDo.of(new WriteFn(this)));
      return PDone.in(input.getPipeline());
    }

    @Override
    public void validate(PipelineOptions pipelineOptions) {
      checkState(uri() != null, "InfluxDbIO.write() requires an URI to be set via withUri(uri)");
      checkState(database() != null, "InfluxDbIO.write() requires a database to be set via "
          + "withDatabase(database)");
    }

    private static class WriteFn extends DoFn<String, Void> {

      private final Write spec;
      private InfluxDB influxDB;

      public WriteFn(Write spec) {
        this.spec = spec;
      }

      @Setup
      public void setup() throws Exception {
        LOG.debug("Connecting to InfluxDB {} at {}", spec.database(), spec.uri());
        if (influxDB == null) {
          influxDB = InfluxDBFactory.connect(spec.uri());
          // TODO do we really want to create a database there ??!!
          influxDB.createDatabase(spec.database());
        }
      }

      // TODO evaluate the batching here
      @ProcessElement
      public void processElement(ProcessContext ctx) throws Exception {
        BatchPoints batchPoint;
        batchPoint = BatchPoints
            .database(spec.database())
            .tag("sflow", ctx.element())
            .retentionPolicy("autogen")
            .consistency(InfluxDB.ConsistencyLevel.ALL)
            .build();
        Point point1 = Point.measurement(spec.database())
            .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
            .addField("sflow", ctx.element())
            .build();
        batchPoint.point(point1);
        influxDB.write(batchPoint);
      }

      @Teardown
      public void teardown() throws Exception {
        influxDB.close();
      }
    }
  }
}