/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.broker.it.clustering;

import static io.zeebe.test.util.TestUtil.waitUntil;

import io.zeebe.broker.Broker;
import io.zeebe.broker.it.DataDeleteTest;
import io.zeebe.broker.it.GrpcClientRule;
import io.zeebe.test.util.TestUtil;
import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class DeletionTest {

  protected void shouldDeleteDataOnLeader(
      ClusteringRule clusteringRule, GrpcClientRule clientRule) {
    // given
    final int leaderNodeId = clusteringRule.getLeaderForPartition(1).getNodeId();
    final Broker leader = clusteringRule.getBroker(leaderNodeId);

    // when
    final AtomicInteger messagesSent = new AtomicInteger();
    while (getSegmentsDirectory(leader).listFiles().length <= 2) {
      clientRule
          .getClient()
          .newPublishMessageCommand()
          .messageName("msg")
          .correlationKey("key")
          .send()
          .join();
      messagesSent.incrementAndGet();
    }

    // when
    final HashMap<Integer, Integer> segmentCount =
        takeSnapshotAndWaitForReplication(Collections.singletonList(leader), clusteringRule);

    // then
    TestUtil.waitUntil(
        () -> getSegmentsDirectory(leader).listFiles().length < segmentCount.get(leaderNodeId));
  }

  protected void shouldDeleteDataOnFollowers(
      ClusteringRule clusteringRule, GrpcClientRule clientRule) {
    // given
    final int leaderNodeId = clusteringRule.getLeaderForPartition(1).getNodeId();
    final List<Broker> followers =
        clusteringRule.getBrokers().stream()
            .filter(b -> b.getConfig().getCluster().getNodeId() != leaderNodeId)
            .collect(Collectors.toList());

    // when
    final AtomicInteger messagesSent = new AtomicInteger();
    while (followers.stream()
        .map(this::getSegmentsDirectory)
        .allMatch(dir -> dir.listFiles().length <= 2)) {
      clientRule
          .getClient()
          .newPublishMessageCommand()
          .messageName("msg")
          .correlationKey("key")
          .send()
          .join();
      messagesSent.incrementAndGet();
    }

    // when
    final HashMap<Integer, Integer> followerSegmentCounts =
        takeSnapshotAndWaitForReplication(followers, clusteringRule);

    // then
    TestUtil.waitUntil(
        () ->
            followers.stream()
                .allMatch(
                    b ->
                        getSegmentsDirectory(b).listFiles().length
                            < followerSegmentCounts.get(b.getConfig().getCluster().getNodeId())));
  }

  protected HashMap<Integer, Integer> takeSnapshotAndWaitForReplication(
      final List<Broker> brokers, ClusteringRule clusteringRule) {
    final HashMap<Integer, Integer> segmentCounts = new HashMap();
    brokers.forEach(
        b -> {
          final int nodeId = b.getConfig().getCluster().getNodeId();
          segmentCounts.put(nodeId, getSegmentsDirectory(b).list().length);
        });

    clusteringRule.getClock().addTime(Duration.ofSeconds(DataDeleteTest.SNAPSHOT_PERIOD_SECONDS));
    brokers.forEach(this::waitForValidSnapshotAtBroker);
    return segmentCounts;
  }

  File getSnapshotsDirectory(Broker broker) {
    final String dataDir = broker.getConfig().getData().getDirectories().get(0);
    return new File(dataDir, "partition-1/state/1_zb-stream-processor/snapshots");
  }

  protected File getSegmentsDirectory(Broker broker) {
    final String dataDir = broker.getConfig().getData().getDirectories().get(0);
    return new File(dataDir, "/partition-1/segments");
  }

  protected void waitForValidSnapshotAtBroker(Broker broker) {
    final File snapshotsDir = getSnapshotsDirectory(broker);

    waitUntil(
        () -> Arrays.stream(snapshotsDir.listFiles()).anyMatch(f -> !f.getName().contains("tmp")));
  }
}
