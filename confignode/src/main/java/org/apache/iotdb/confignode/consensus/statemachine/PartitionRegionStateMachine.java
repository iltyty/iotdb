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
package org.apache.iotdb.confignode.consensus.statemachine;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.exception.physical.UnknownPhysicalPlanTypeException;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.persistence.executor.ConfigPlanExecutor;
import org.apache.iotdb.confignode.writelog.io.SingleFileLogReader;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.request.ByteBufferConsensusRequest;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.db.utils.writelog.LogWriter;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.TimeUnit;

/** StateMachine for PartitionRegion */
public class PartitionRegionStateMachine
    implements IStateMachine, IStateMachine.EventApi, IStateMachine.RetryPolicy {

  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionRegionStateMachine.class);
  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();
  private final ConfigPlanExecutor executor;
  private ConfigManager configManager;
  private LogWriter logWriter;
  private File logFile;
  private int startIndex;

  private int endIndex;
  private int logFileId;

  private static final String fileDir = CONF.getConsensusDir() + File.separator + "standalone";
  private static final String fileTempPath = fileDir + File.separator + "log_inprogress_";
  private static final String filePath = fileDir + File.separator + "log_";
  private static final long FILE_MAX_SIZE = CONF.getPartitionRegionStandAloneLogSegmentSizeMax();
  private final TEndPoint currentNodeTEndPoint;

  public PartitionRegionStateMachine(ConfigManager configManager, ConfigPlanExecutor executor) {
    this.executor = executor;
    this.configManager = configManager;
    this.currentNodeTEndPoint =
        new TEndPoint()
            .setIp(ConfigNodeDescriptor.getInstance().getConf().getInternalAddress())
            .setPort(ConfigNodeDescriptor.getInstance().getConf().getConsensusPort());
  }

  public ConfigManager getConfigManager() {
    return configManager;
  }

  public void setConfigManager(ConfigManager configManager) {
    this.configManager = configManager;
  }

  @Override
  public TSStatus write(IConsensusRequest request) {
    ConfigPhysicalPlan plan;
    if (request instanceof ByteBufferConsensusRequest) {
      try {
        plan = ConfigPhysicalPlan.Factory.create(request.serializeToByteBuffer());
      } catch (Throwable e) {
        LOGGER.error(
            "Deserialization error for write plan, request: {}, bytebuffer: {}",
            request,
            request.serializeToByteBuffer(),
            e);
        return new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      }
    } else if (request instanceof ConfigPhysicalPlan) {
      plan = (ConfigPhysicalPlan) request;
    } else {
      LOGGER.error(
          "Unexpected write plan, request: {}, bytebuffer: {}",
          request,
          request.serializeToByteBuffer());
      return new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    }
    return write(plan);
  }

  /** Transmit PhysicalPlan to confignode.service.executor.PlanExecutor */
  protected TSStatus write(ConfigPhysicalPlan plan) {
    TSStatus result;
    try {
      result = executor.executeNonQueryPlan(plan);
    } catch (UnknownPhysicalPlanTypeException | AuthException e) {
      LOGGER.error(e.getMessage());
      result = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    }

    if (ConsensusFactory.StandAloneConsensus.equals(CONF.getConfigNodeConsensusProtocolClass())) {
      if (logFile.length() > FILE_MAX_SIZE) {
        try {
          logWriter.force();
          File fileDir = new File(filePath);
          Files.move(logFile.toPath(), fileDir.toPath(), StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
          LOGGER.error("Can't force logWrite for ConfigNode Standalone mode", e);
        }
        for (int retry = 0; retry < 5; retry++) {
          try {
            logWriter.close();
          } catch (IOException e) {
            LOGGER.warn(
                "Can't close StandAloneLog for ConfigNode Standalone mode, filePath: {}, retry: {}",
                logFile.getAbsolutePath(),
                retry);
            try {
              // Sleep 1s and retry
              TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e2) {
              Thread.currentThread().interrupt();
              LOGGER.warn("Unexpected interruption during the close method of logWriter");
            }
            continue;
          }
          break;
        }
        createLogFile(logFileId + 1);
      }

      try {
        ByteBuffer buffer = plan.serializeToByteBuffer();
        // The method logWriter.write will execute flip() firstly, so we must make position==limit
        buffer.position(buffer.limit());
        logWriter.write(buffer);
        logFileId=logFileId+1;
        File logFileTmp=new File(fileTempPath+logFileId);
        Files.move(logFile.toPath(), logFileTmp.toPath(), StandardCopyOption.ATOMIC_MOVE);
      } catch (IOException e) {
        LOGGER.error(
            "can't serialize current ConfigPhysicalPlan for ConfigNode Standalone mode", e);
      }
    }
    return result;
  }

  @Override
  public DataSet read(IConsensusRequest request) {
    ConfigPhysicalPlan plan;
    if (request instanceof ByteBufferConsensusRequest) {
      try {
        plan = ConfigPhysicalPlan.Factory.create(request.serializeToByteBuffer());
      } catch (Throwable e) {
        LOGGER.error("Deserialization error for write plan : {}", request);
        return null;
      }
    } else if (request instanceof ConfigPhysicalPlan) {
      plan = (ConfigPhysicalPlan) request;
    } else {
      LOGGER.error("Unexpected read plan : {}", request);
      return null;
    }
    return read(plan);
  }

  @Override
  public boolean takeSnapshot(File snapshotDir) {
    return executor.takeSnapshot(snapshotDir);
  }

  @Override
  public void loadSnapshot(File latestSnapshotRootDir) {
    executor.loadSnapshot(latestSnapshotRootDir);
  }

  /** Transmit PhysicalPlan to confignode.service.executor.PlanExecutor */
  protected DataSet read(ConfigPhysicalPlan plan) {
    DataSet result;
    try {
      result = executor.executeQueryPlan(plan);
    } catch (UnknownPhysicalPlanTypeException | AuthException e) {
      LOGGER.error(e.getMessage());
      result = null;
    }
    return result;
  }

  @Override
  public void notifyLeaderChanged(ConsensusGroupId groupId, int newLeaderId) {
    // We get currentNodeId here because the currentNodeId
    // couldn't initialize earlier than the PartitionRegionStateMachine
    int currentNodeId = ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId();

    if (currentNodeId == newLeaderId) {
      LOGGER.info(
          "Current node [nodeId: {}, ip:port: {}] becomes Leader",
          newLeaderId,
          currentNodeTEndPoint);
      configManager.getProcedureManager().shiftExecutor(true);
      configManager.getLoadManager().startLoadBalancingService();
      configManager.getNodeManager().startHeartbeatService();
      configManager.getNodeManager().startUnknownDataNodeDetector();
      configManager.getPartitionManager().startRegionCleaner();
    } else {
      LOGGER.info(
          "Current node [nodeId:{}, ip:port: {}] is not longer the leader, the new leader is [nodeId:{}]",
          currentNodeId,
          currentNodeTEndPoint,
          newLeaderId);
      configManager.getProcedureManager().shiftExecutor(false);
      configManager.getLoadManager().stopLoadBalancingService();
      configManager.getNodeManager().stopHeartbeatService();
      configManager.getNodeManager().stopUnknownDataNodeDetector();
      configManager.getPartitionManager().stopRegionCleaner();
    }
  }

  @Override
  public void start() {
    if (ConsensusFactory.StandAloneConsensus.equals(CONF.getConfigNodeConsensusProtocolClass())) {
      initStandAloneConfigNode();
    }
  }

  @Override
  public void stop() {
    // do nothing
  }

  @Override
  public boolean isReadOnly() {
    return CommonDescriptor.getInstance().getConfig().isReadOnly();
  }

  @Override
  public boolean shouldRetry(TSStatus writeResult) {
    // TODO implement this
    return RetryPolicy.super.shouldRetry(writeResult);
  }

  @Override
  public TSStatus updateResult(TSStatus previousResult, TSStatus retryResult) {
    // TODO implement this
    return RetryPolicy.super.updateResult(previousResult, retryResult);
  }

  @Override
  public long getSleepTime() {
    // TODO implement this
    return RetryPolicy.super.getSleepTime();
  }

  private void initStandAloneConfigNode() {
    File dir = new File(fileDir);
    dir.mkdir();
    String[] list = new File(fileDir).list();
    if (list != null && list.length != 0) {
      for (String logFileName : list) {
        if (logFileName.startsWith("log_inprogress")) {
          logFileId =
              Integer.parseInt(
                  logFileName.substring(logFileName.lastIndexOf("_") + 1, logFileName.length()));
        }
        if (startIndex<Integer.parseInt(logFileName.substring(logFileName.lastIndexOf("_")+1,logFileName.length())))
        {
          startIndex=Integer.parseInt(logFileName.substring(logFileName.lastIndexOf("_")+1,logFileName.length()));
        }
        File logFile = SystemFileFactory.INSTANCE.getFile(fileDir + File.separator + logFileName);
        SingleFileLogReader logReader;
        try {
          logReader = new SingleFileLogReader(logFile);
        } catch (FileNotFoundException e) {
          LOGGER.error(
              "initStandAloneConfigNode meets error, can't find standalone log files, filePath: {}",
              logFile.getAbsolutePath(),
              e);
          continue;
        }
        while (logReader.hasNext()) {
          // read and re-serialize the PhysicalPlan
          ConfigPhysicalPlan nextPlan = logReader.next();
          try {
            executor.executeNonQueryPlan(nextPlan);
          } catch (UnknownPhysicalPlanTypeException | AuthException e) {
            LOGGER.error(e.getMessage());
          }
        }
        logReader.close();
      }
    } else {
      startIndex=0;
      logFileId = 0;
    }
    endIndex = logFileId;
    File logFiletmp=new File(fileTempPath+logFileId);
    if (logFiletmp.exists()){
    logFile=new File(filePath+startIndex+"_"+endIndex);
    try {
      Files.move(logFiletmp.toPath(), logFile.toPath(), StandardCopyOption.ATOMIC_MOVE);
    } catch (IOException e) {
      LOGGER.error(
              "can't serialize current ConfigPhysicalPlan for ConfigNode Standalone mode", e);
    }}
    createLogFile(endIndex+1);
  }

  private void createLogFile(int logFileId) {
    logFile = SystemFileFactory.INSTANCE.getFile(fileTempPath + logFileId);
    try {
      if (logFile.createNewFile()) {
        logWriter = new LogWriter(logFile, false);
        LOGGER.info("Create StandaloneLog: {}", logFile.getAbsolutePath());
      }
    } catch (IOException e) {
      LOGGER.warn("Can't create StandaloneLog: {}, retrying...", logFile.getAbsolutePath());
      try {
        TimeUnit.SECONDS.sleep(1);
      } catch (InterruptedException ignored) {
        // Ignore and retry
      }
    }
  }
}
