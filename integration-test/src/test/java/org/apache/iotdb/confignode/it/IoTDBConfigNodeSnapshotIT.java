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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.confignode.it;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.cq.CQState;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathDeserializeUtil;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.trigger.TriggerInformation;
import org.apache.iotdb.commons.trigger.service.TriggerExecutableManager;
import org.apache.iotdb.confignode.rpc.thrift.TCQEntry;
import org.apache.iotdb.confignode.rpc.thrift.TCreateCQReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateTriggerReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetTriggerTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TSetStorageGroupReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowCQResp;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.ConfigFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.trigger.api.enums.FailureStrategy;
import org.apache.iotdb.trigger.api.enums.TriggerEvent;
import org.apache.iotdb.trigger.api.enums.TriggerType;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBConfigNodeSnapshotIT {

  protected static String originalConfigNodeConsensusProtocolClass;

  protected static int originalRatisSnapshotTriggerThreshold;
  private static final int testRatisSnapshotTriggerThreshold = 10;

  protected static long originalTimePartitionInterval;
  private static final long testTimePartitionInterval = 86400;

  @Before
  public void setUp() throws Exception {
    originalConfigNodeConsensusProtocolClass =
        ConfigFactory.getConfig().getConfigNodeConsesusProtocolClass();
    ConfigFactory.getConfig().setConfigNodeConsesusProtocolClass(ConsensusFactory.RatisConsensus);

    originalRatisSnapshotTriggerThreshold =
        ConfigFactory.getConfig().getRatisSnapshotTriggerThreshold();
    ConfigFactory.getConfig().setRatisSnapshotTriggerThreshold(testRatisSnapshotTriggerThreshold);

    originalTimePartitionInterval = ConfigFactory.getConfig().getTimePartitionInterval();
    ConfigFactory.getConfig().setTimePartitionIntervalForRouting(testTimePartitionInterval);

    // Init 3C3D cluster environment
    EnvFactory.getEnv().initClusterEnvironment(3, 3);
  }

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanAfterClass();

    ConfigFactory.getConfig()
        .setConfigNodeConsesusProtocolClass(originalConfigNodeConsensusProtocolClass);
    ConfigFactory.getConfig()
        .setRatisSnapshotTriggerThreshold(originalRatisSnapshotTriggerThreshold);
    ConfigFactory.getConfig().setTimePartitionIntervalForRouting(originalTimePartitionInterval);
  }

  private ByteBuffer generatePatternTreeBuffer(String path)
      throws IllegalPathException, IOException {
    PathPatternTree patternTree = new PathPatternTree();
    patternTree.appendPathPattern(new PartialPath(path));
    patternTree.constructTree();

    PublicBAOS baos = new PublicBAOS();
    patternTree.serialize(baos);
    return ByteBuffer.wrap(baos.toByteArray());
  }

  @Test
  public void testPartitionInfoSnapshot()
      throws IOException, IllegalPathException, TException, InterruptedException {
    final String sg = "root.sg";
    final int storageGroupNum = 10;
    final int seriesPartitionSlotsNum = 10;
    final int timePartitionSlotsNum = 10;

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {

      List<TCreateTriggerReq> createTriggerReqs = createTrigger(client);

      Set<TCQEntry> expectedCQEntries = createCQs(client);

      for (int i = 0; i < storageGroupNum; i++) {
        String storageGroup = sg + i;
        TSetStorageGroupReq setStorageGroupReq =
            new TSetStorageGroupReq(new TStorageGroupSchema(storageGroup));
        TSStatus status = client.setStorageGroup(setStorageGroupReq);
        assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

        for (int j = 0; j < seriesPartitionSlotsNum; j++) {
          TSeriesPartitionSlot seriesPartitionSlot = new TSeriesPartitionSlot(j);

          // Create SchemaPartition
          ByteBuffer patternTree = generatePatternTreeBuffer(storageGroup + ".d" + j + ".s");
          TSchemaPartitionReq schemaPartitionReq = new TSchemaPartitionReq(patternTree);
          TSchemaPartitionTableResp schemaPartitionTableResp =
              client.getOrCreateSchemaPartitionTable(schemaPartitionReq);
          // All requests should success if snapshot success
          assertEquals(
              TSStatusCode.SUCCESS_STATUS.getStatusCode(),
              schemaPartitionTableResp.getStatus().getCode());
          Assert.assertNotNull(schemaPartitionTableResp.getSchemaPartitionTable());
          assertEquals(1, schemaPartitionTableResp.getSchemaPartitionTableSize());
          Assert.assertNotNull(
              schemaPartitionTableResp.getSchemaPartitionTable().get(storageGroup));
          assertEquals(
              1, schemaPartitionTableResp.getSchemaPartitionTable().get(storageGroup).size());

          for (int k = 0; k < timePartitionSlotsNum; k++) {
            TTimePartitionSlot timePartitionSlot =
                new TTimePartitionSlot(testTimePartitionInterval * k);

            // Create DataPartition
            Map<String, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>> partitionSlotsMap =
                new HashMap<>();
            partitionSlotsMap.put(storageGroup, new HashMap<>());
            partitionSlotsMap
                .get(storageGroup)
                .put(seriesPartitionSlot, Collections.singletonList(timePartitionSlot));
            TDataPartitionReq dataPartitionReq = new TDataPartitionReq(partitionSlotsMap);
            TDataPartitionTableResp dataPartitionTableResp =
                client.getOrCreateDataPartitionTable(dataPartitionReq);
            // All requests should success if snapshot success
            assertEquals(
                TSStatusCode.SUCCESS_STATUS.getStatusCode(),
                dataPartitionTableResp.getStatus().getCode());
            Assert.assertNotNull(dataPartitionTableResp.getDataPartitionTable());
            assertEquals(1, dataPartitionTableResp.getDataPartitionTableSize());
            Assert.assertNotNull(dataPartitionTableResp.getDataPartitionTable().get(storageGroup));
            assertEquals(
                1, dataPartitionTableResp.getDataPartitionTable().get(storageGroup).size());
            Assert.assertNotNull(
                dataPartitionTableResp
                    .getDataPartitionTable()
                    .get(storageGroup)
                    .get(seriesPartitionSlot));
            assertEquals(
                1,
                dataPartitionTableResp
                    .getDataPartitionTable()
                    .get(storageGroup)
                    .get(seriesPartitionSlot)
                    .size());
          }
        }
      }

      assertTriggerInformation(createTriggerReqs, client.getTriggerTable());

      TShowCQResp showCQResp = client.showCQ();
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), showCQResp.getStatus().getCode());
      assertEquals(expectedCQEntries, new HashSet<>(showCQResp.cqList));
    }
  }

  private List<TCreateTriggerReq> createTrigger(SyncConfigNodeIServiceClient client)
      throws IllegalPathException, TException, IOException {
    final String triggerPath =
        System.getProperty("user.dir")
            + File.separator
            + "target"
            + File.separator
            + "test-classes"
            + File.separator
            + "trigger-example.jar";
    ByteBuffer jarFile = TriggerExecutableManager.transferToBytebuffer(triggerPath);
    String jarMD5 = DigestUtils.md5Hex(jarFile.array());

    TCreateTriggerReq createTriggerReq1 =
        new TCreateTriggerReq(
                "test1",
                "org.apache.iotdb.trigger.SimpleTrigger",
                "trigger-example.jar",
                false,
                TriggerEvent.AFTER_INSERT.getId(),
                TriggerType.STATELESS.getId(),
                new PartialPath("root.test1.**").serialize(),
                Collections.emptyMap(),
                FailureStrategy.OPTIMISTIC.getId())
            .setJarMD5(jarMD5)
            .setJarFile(jarFile);

    Map<String, String> attributes = new HashMap<>();
    attributes.put("test-key", "test-value");
    TCreateTriggerReq createTriggerReq2 =
        new TCreateTriggerReq(
                "test2",
                "org.apache.iotdb.trigger.SimpleTrigger",
                "trigger-example.jar",
                false,
                TriggerEvent.BEFORE_INSERT.getId(),
                TriggerType.STATEFUL.getId(),
                new PartialPath("root.test2.**").serialize(),
                attributes,
                FailureStrategy.OPTIMISTIC.getId())
            .setJarMD5(jarMD5)
            .setJarFile(jarFile);

    assertEquals(
        client.createTrigger(createTriggerReq1).getCode(),
        TSStatusCode.SUCCESS_STATUS.getStatusCode());
    assertEquals(
        client.createTrigger(createTriggerReq2).getCode(),
        TSStatusCode.SUCCESS_STATUS.getStatusCode());

    List<TCreateTriggerReq> result = new ArrayList<>();
    result.add(createTriggerReq2);
    result.add(createTriggerReq1);
    return result;
  }

  private void assertTriggerInformation(List<TCreateTriggerReq> req, TGetTriggerTableResp resp) {
    for (int i = 0; i < req.size(); i++) {
      TCreateTriggerReq createTriggerReq = req.get(i);
      TriggerInformation triggerInformation =
          TriggerInformation.deserialize(resp.getAllTriggerInformation().get(i));

      assertEquals(createTriggerReq.getTriggerName(), triggerInformation.getTriggerName());
      assertEquals(createTriggerReq.getClassName(), triggerInformation.getClassName());
      assertEquals(createTriggerReq.getJarPath(), triggerInformation.getJarName());
      assertEquals(createTriggerReq.getTriggerEvent(), triggerInformation.getEvent().getId());
      assertEquals(
          createTriggerReq.getTriggerType(),
          triggerInformation.isStateful()
              ? TriggerType.STATEFUL.getId()
              : TriggerType.STATELESS.getId());
      assertEquals(
          PathDeserializeUtil.deserialize(ByteBuffer.wrap(createTriggerReq.getPathPattern())),
          triggerInformation.getPathPattern());
    }
  }

  private Set<TCQEntry> createCQs(SyncConfigNodeIServiceClient client) throws TException {
    String sql1 = "create cq testCq1 BEGIN select s1 into root.backup.d1(s1) from root.sg.d1 END";
    String sql2 = "create cq testCq2 BEGIN select s1 into root.backup.d2(s1) from root.sg.d2 END";
    TCreateCQReq req1 =
        new TCreateCQReq(
            "testCq1",
            1000,
            0,
            1000,
            0,
            (byte) 0,
            "select s1 into root.backup.d1(s1) from root.sg.d1",
            sql1,
            "Asia");
    TCreateCQReq req2 =
        new TCreateCQReq(
            "testCq2",
            1000,
            0,
            1000,
            0,
            (byte) 1,
            "select s1 into root.backup.d2(s1) from root.sg.d2",
            sql2,
            "Asia");

    assertEquals(client.createCQ(req1).getCode(), TSStatusCode.SUCCESS_STATUS.getStatusCode());
    assertEquals(client.createCQ(req2).getCode(), TSStatusCode.SUCCESS_STATUS.getStatusCode());

    Set<TCQEntry> result = new HashSet<>();
    result.add(new TCQEntry("testCq1", sql1, CQState.ACTIVE.getType()));
    result.add(new TCQEntry("testCq2", sql2, CQState.ACTIVE.getType()));
    return result;
  }
}
