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

package org.apache.beam.runners.direct;

import static org.apache.beam.sdk.metrics.MetricMatchers.attemptedMetricsResult;
import static org.apache.beam.sdk.metrics.MetricMatchers.committedMetricsResult;
import static org.apache.beam.sdk.metrics.MetricNameFilter.inNamespace;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import org.apache.beam.runners.direct.DirectRunner.CommittedBundle;
import org.apache.beam.sdk.metrics.DistributionData;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricUpdates;
import org.apache.beam.sdk.metrics.MetricUpdates.MetricUpdate;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Tests for {@link DirectMetrics}.
 */
@RunWith(JUnit4.class)
public class DirectMetricsTest {

  @Mock
  private CommittedBundle<Object> bundle1;
  @Mock
  private CommittedBundle<Object> bundle2;

  private static final MetricName NAME1 = MetricName.named("ns1", "name1");
  private static final MetricName NAME2 = MetricName.named("ns1", "name2");
  private static final MetricName NAME3 = MetricName.named("ns2", "name1");

  private DirectMetrics metrics = new DirectMetrics();

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testApplyCommittedNoFilter() {
    metrics.commitLogical(bundle1, MetricUpdates.create(
        ImmutableList.of(
            MetricUpdate.create(MetricKey.create("step1", NAME1), 5L),
            MetricUpdate.create(MetricKey.create("step1", NAME2), 8L)),
        ImmutableList.of(
            MetricUpdate.create(MetricKey.create("step1", NAME1),
                DistributionData.create(8, 2, 3, 5)))));
    metrics.commitLogical(bundle1, MetricUpdates.create(
        ImmutableList.of(
            MetricUpdate.create(MetricKey.create("step2", NAME1), 7L),
            MetricUpdate.create(MetricKey.create("step1", NAME2), 4L)),
        ImmutableList.of(
            MetricUpdate.create(MetricKey.create("step1", NAME1),
                DistributionData.create(4, 1, 4, 4)))));

    MetricQueryResults results = metrics.queryMetrics(MetricsFilter.builder().build());
    assertThat(results.counters(), containsInAnyOrder(
        attemptedMetricsResult("ns1", "name1", "step1", 0L),
        attemptedMetricsResult("ns1", "name2", "step1", 0L),
        attemptedMetricsResult("ns1", "name1", "step2", 0L)));
    assertThat(results.counters(), containsInAnyOrder(
        committedMetricsResult("ns1", "name1", "step1", 5L),
        committedMetricsResult("ns1", "name2", "step1", 12L),
        committedMetricsResult("ns1", "name1", "step2", 7L)));
    assertThat(results.distributions(), contains(
        attemptedMetricsResult("ns1", "name1", "step1", DistributionResult.ZERO)));
    assertThat(results.distributions(), contains(
        committedMetricsResult("ns1", "name1", "step1", DistributionResult.create(12, 3, 3, 5))));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testApplyAttemptedCountersQueryOneNamespace() {
    metrics.updatePhysical(bundle1, MetricUpdates.create(
        ImmutableList.of(
            MetricUpdate.create(MetricKey.create("step1", NAME1), 5L),
            MetricUpdate.create(MetricKey.create("step1", NAME3), 8L)),
        ImmutableList.<MetricUpdate<DistributionData>>of()));
    metrics.updatePhysical(bundle1, MetricUpdates.create(
        ImmutableList.of(
            MetricUpdate.create(MetricKey.create("step2", NAME1), 7L),
            MetricUpdate.create(MetricKey.create("step1", NAME3), 4L)),
        ImmutableList.<MetricUpdate<DistributionData>>of()));

    MetricQueryResults results = metrics.queryMetrics(
        MetricsFilter.builder().addNameFilter(inNamespace("ns1")).build());

    assertThat(results.counters(),
        containsInAnyOrder(
            attemptedMetricsResult("ns1", "name1", "step1", 5L),
            attemptedMetricsResult("ns1", "name1", "step2", 7L)));

    assertThat(results.counters(),
        containsInAnyOrder(
            committedMetricsResult("ns1", "name1", "step1", 0L),
            committedMetricsResult("ns1", "name1", "step2", 0L)));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testApplyAttemptedQueryCompositeScope() {
    metrics.updatePhysical(bundle1, MetricUpdates.create(
        ImmutableList.of(
            MetricUpdate.create(MetricKey.create("Outer1/Inner1", NAME1), 5L),
            MetricUpdate.create(MetricKey.create("Outer1/Inner2", NAME1), 8L)),
        ImmutableList.<MetricUpdate<DistributionData>>of()));
    metrics.updatePhysical(bundle1, MetricUpdates.create(
        ImmutableList.of(
            MetricUpdate.create(MetricKey.create("Outer1/Inner1", NAME1), 12L),
            MetricUpdate.create(MetricKey.create("Outer2/Inner2", NAME1), 18L)),
        ImmutableList.<MetricUpdate<DistributionData>>of()));

    MetricQueryResults results = metrics.queryMetrics(
        MetricsFilter.builder().addStep("Outer1").build());

    assertThat(results.counters(),
        containsInAnyOrder(
            attemptedMetricsResult("ns1", "name1", "Outer1/Inner1", 12L),
            attemptedMetricsResult("ns1", "name1", "Outer1/Inner2", 8L)));

    assertThat(results.counters(),
        containsInAnyOrder(
            committedMetricsResult("ns1", "name1", "Outer1/Inner1", 0L),
            committedMetricsResult("ns1", "name1", "Outer1/Inner2", 0L)));
  }


  @SuppressWarnings("unchecked")
  @Test
  public void testPartialScopeMatchingInMetricsQuery() {
    metrics.updatePhysical(bundle1, MetricUpdates.create(
        ImmutableList.of(
            MetricUpdate.create(MetricKey.create("Top1/Outer1/Inner1", NAME1), 5L),
            MetricUpdate.create(MetricKey.create("Top1/Outer1/Inner2", NAME1), 8L)),
        ImmutableList.<MetricUpdate<DistributionData>>of()));
    metrics.updatePhysical(bundle1, MetricUpdates.create(
        ImmutableList.of(
            MetricUpdate.create(MetricKey.create("Top2/Outer1/Inner1", NAME1), 12L),
            MetricUpdate.create(MetricKey.create("Top1/Outer2/Inner2", NAME1), 18L)),
        ImmutableList.<MetricUpdate<DistributionData>>of()));

    MetricQueryResults results = metrics.queryMetrics(
        MetricsFilter.builder().addStep("Top1/Outer1").build());

    assertThat(results.counters(),
        containsInAnyOrder(
            attemptedMetricsResult("ns1", "name1", "Top1/Outer1/Inner1", 5L),
            attemptedMetricsResult("ns1", "name1", "Top1/Outer1/Inner2", 8L)));

    results = metrics.queryMetrics(
        MetricsFilter.builder().addStep("Inner2").build());

    assertThat(results.counters(),
        containsInAnyOrder(
            attemptedMetricsResult("ns1", "name1", "Top1/Outer1/Inner2", 8L),
            attemptedMetricsResult("ns1", "name1", "Top1/Outer2/Inner2", 18L)));
  }
}
