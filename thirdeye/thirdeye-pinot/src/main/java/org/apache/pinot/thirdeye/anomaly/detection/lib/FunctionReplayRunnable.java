/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.thirdeye.anomaly.detection.lib;

import org.apache.pinot.thirdeye.anomaly.detection.DetectionJobScheduler;
import org.apache.pinot.thirdeye.anomalydetection.performanceEvaluation.PerformanceEvaluate;
import org.apache.pinot.thirdeye.anomalydetection.performanceEvaluation.PerformanceEvaluateHelper;
import org.apache.pinot.thirdeye.anomalydetection.performanceEvaluation.PerformanceEvaluationMethod;
import org.apache.pinot.thirdeye.dashboard.resources.OnboardResource;
import org.apache.pinot.thirdeye.datalayer.bao.AnomalyFunctionManager;
import org.apache.pinot.thirdeye.datalayer.bao.AutotuneConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import org.apache.pinot.thirdeye.datalayer.dto.AutotuneConfigDTO;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FunctionReplayRunnable implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(FunctionReplayRunnable.class);
  private DetectionJobScheduler detectionJobScheduler;
  private MergedAnomalyResultManager mergedAnomalyResultDAO;
  private AnomalyFunctionManager anomalyFunctionDAO;
  private AutotuneMethodType autotuneMethodType;
  private AutotuneConfigManager autotuneConfigDAO;
  private PerformanceEvaluationMethod performanceEvaluationMethod;
  private long tuningFunctionId;
  private DateTime replayStart;
  private DateTime replayEnd;
  private double goal;
  private boolean isForceBackfill;
  private Map<String, String> tuningParameter;
  private Long functionAutotuneConfigId;
  private boolean speedUp;
  private boolean selfKill;
  private long lastClonedFunctionId;

  public FunctionReplayRunnable(DetectionJobScheduler detectionJobScheduler, AnomalyFunctionManager anomalyFunctionDAO,
      MergedAnomalyResultManager mergedAnomalyResultDAO, AutotuneConfigManager autotuneConfigDAO) {
    this.detectionJobScheduler = detectionJobScheduler;
    this.mergedAnomalyResultDAO = mergedAnomalyResultDAO;
    this.anomalyFunctionDAO = anomalyFunctionDAO;
    this.autotuneConfigDAO = autotuneConfigDAO;
    setSpeedUp(true);
    setForceBackfill(true);
    setSelfKill(true);
  }

  public FunctionReplayRunnable(DetectionJobScheduler detectionJobScheduler, AnomalyFunctionManager anomalyFunctionDAO,
      MergedAnomalyResultManager mergedAnomalyResultDAO, AutotuneConfigManager autotuneConfigDAO,
      Map<String, String> tuningParameter, long tuningFunctionId, DateTime replayStart, DateTime replayEnd, double goal,
      long functionAutotuneConfigId, boolean isForceBackfill, boolean selfKill) {
    this(detectionJobScheduler, anomalyFunctionDAO, mergedAnomalyResultDAO, autotuneConfigDAO);
    setTuningFunctionId(tuningFunctionId);
    setReplayStart(replayStart);
    setReplayEnd(replayEnd);
    setForceBackfill(isForceBackfill);
    setTuningParameter(tuningParameter);
    setFunctionAutotuneConfigId(functionAutotuneConfigId);
    setSpeedUp(true);
    setGoal(goal);
    setSelfKill(selfKill);
  }

  public FunctionReplayRunnable(DetectionJobScheduler detectionJobScheduler, AnomalyFunctionManager anomalyFunctionDAO,
      MergedAnomalyResultManager mergedAnomalyResultDAO, Map<String, String> tuningParameter, long tuningFunctionId,
      DateTime replayStart, DateTime replayEnd, boolean selfKill) {
    this(detectionJobScheduler, anomalyFunctionDAO, mergedAnomalyResultDAO, null);
    setTuningFunctionId(tuningFunctionId);
    setReplayStart(replayStart);
    setReplayEnd(replayEnd);
    setForceBackfill(true);
    setTuningParameter(tuningParameter);
    setSpeedUp(true);
    setSelfKill(selfKill);
  }

  public static void speedup(AnomalyFunctionDTO anomalyFunctionDTO) {
    switch (anomalyFunctionDTO.getWindowUnit()) {
      case NANOSECONDS:
      case MICROSECONDS:
      case MILLISECONDS:
      case SECONDS:
      case MINUTES:       // These TimeUnits are not currently in use
      case HOURS:
      case DAYS:
          /*
          SignTest takes HOURS data, but changing to 7 days won't affect the final result
          SPLINE takes 1 DAYS data, for heuristic, we extend it to 7 days.
           */
      default:
        anomalyFunctionDTO.setWindowSize(7);
        anomalyFunctionDTO.setWindowUnit(TimeUnit.DAYS);
        anomalyFunctionDTO.setCron("0 0 0 ? * MON *");
    }
  }

  @Override
  public void run() {
    long currentTime = System.currentTimeMillis();
    long clonedFunctionId = 0l;
    OnboardResource onboardResource = new OnboardResource(anomalyFunctionDAO, mergedAnomalyResultDAO);
    StringBuilder functionName = new StringBuilder("clone");
    for (Map.Entry<String, String> entry : tuningParameter.entrySet()) {
      functionName.append("_");
      functionName.append(entry.getKey());
      functionName.append("_");
      functionName.append(entry.getValue());
    }
    try {
      clonedFunctionId = onboardResource.cloneAnomalyFunctionById(tuningFunctionId, functionName.toString(), false);
      this.lastClonedFunctionId = clonedFunctionId;
    }
    catch (Exception e) {
      LOG.error("Unable to clone function {} with given name {}", tuningFunctionId, functionName.toString(), e);
      return;
    }

    AnomalyFunctionDTO anomalyFunctionDTO = anomalyFunctionDAO.findById(clonedFunctionId);
    // Remove alert filters
    anomalyFunctionDTO.setAlertFilter(null);

    int originWindowSize = anomalyFunctionDTO.getWindowSize();
    TimeUnit originWindowUnit = anomalyFunctionDTO.getWindowUnit();
    String originCron = anomalyFunctionDTO.getCron();

    // enlarge window size so that we can speed-up the replay speed
    if(speedUp) {
      FunctionReplayRunnable.speedup(anomalyFunctionDTO);
    }

    // Set Properties
    anomalyFunctionDTO.updateProperties(tuningParameter);
    anomalyFunctionDTO.setActive(true);

    anomalyFunctionDAO.update(anomalyFunctionDTO);

    List<Long> functionIdList = new ArrayList<>();
    functionIdList.add(clonedFunctionId);
    detectionJobScheduler.synchronousBackFill(functionIdList, replayStart, replayEnd, isForceBackfill);

    if(autotuneConfigDAO != null) { // if no functionAutotuneId, skip update
      PerformanceEvaluate performanceEvaluator =
          PerformanceEvaluateHelper.getPerformanceEvaluator(performanceEvaluationMethod, tuningFunctionId,
              clonedFunctionId, new Interval(replayStart.getMillis(), replayEnd.getMillis()), mergedAnomalyResultDAO);
      double performance = performanceEvaluator.evaluate();

      AutotuneConfigDTO targetAutotuneDTO = autotuneConfigDAO.findById(functionAutotuneConfigId);

      Map<String, Double> prevPerformance = targetAutotuneDTO.getPerformance();
      // if there is no previous performance, update performance directly
      // Otherwise, compare the performance, and update if betterW
      if (prevPerformance == null || prevPerformance.isEmpty() ||
          Math.abs(prevPerformance.get(performanceEvaluationMethod.name()) - goal) > Math.abs(performance - goal)) {
        targetAutotuneDTO.setConfiguration(tuningParameter);
        Map<String, Double> newPerformance = targetAutotuneDTO.getPerformance();
        newPerformance.put(performanceEvaluationMethod.name(), performance);
        targetAutotuneDTO.setPerformance(newPerformance);
        targetAutotuneDTO.setAvgRunningTime((System.currentTimeMillis() - currentTime) / 1000);
        targetAutotuneDTO.setLastUpdateTimestamp(System.currentTimeMillis());
      }
      String message = (targetAutotuneDTO.getMessage().isEmpty()) ? "" : (targetAutotuneDTO.getMessage() + ";");

      targetAutotuneDTO.setMessage(message + tuningParameter.toString() + ":" + performance);

      autotuneConfigDAO.update(targetAutotuneDTO);
    }

    // clean up and kill itself
    if(selfKill) {
      onboardResource.deleteExistingAnomalies(clonedFunctionId, replayStart.getMillis(),
          replayEnd.getMillis());
      anomalyFunctionDAO.deleteById(clonedFunctionId);
    } else {
      anomalyFunctionDTO.setWindowSize(originWindowSize);
      anomalyFunctionDTO.setWindowUnit(originWindowUnit);
      anomalyFunctionDTO.setCron(originCron);
      anomalyFunctionDAO.update(anomalyFunctionDTO);
    }
  }


  public long getTuningFunctionId() {
    return tuningFunctionId;
  }

  public void setTuningFunctionId(long functionId) {
    this.tuningFunctionId = functionId;
  }

  public DateTime getReplayStart() {
    return replayStart;
  }

  public void setReplayStart(DateTime replayStart) {
    this.replayStart = replayStart;
  }

  public DateTime getReplayEnd() {
    return replayEnd;
  }

  public void setReplayEnd(DateTime replayEnd) {
    this.replayEnd = replayEnd;
  }

  public boolean isForceBackfill() {
    return isForceBackfill;
  }

  public void setForceBackfill(boolean forceBackfill) {
    isForceBackfill = forceBackfill;
  }

  public Map<String, String> getTuningParameter() {
    return tuningParameter;
  }

  public void setTuningParameter(Map<String, String> tuningParameter) {
    this.tuningParameter = tuningParameter;
  }

  public AutotuneMethodType getAutotuneMethodType() {
    return autotuneMethodType;
  }

  public void setAutotuneMethodType(AutotuneMethodType autotuneMethodType) {
    this.autotuneMethodType = autotuneMethodType;
  }

  public PerformanceEvaluationMethod getPerformanceEvaluationMethod() {
    return performanceEvaluationMethod;
  }

  public void setPerformanceEvaluationMethod(PerformanceEvaluationMethod performanceEvaluationMethod) {
    this.performanceEvaluationMethod = performanceEvaluationMethod;
  }

  public double getGoal() {
    return goal;
  }

  public void setGoal(double goal) {
    this.goal = goal;
  }

  public Long getFunctionAutotuneConfigId() {
    return functionAutotuneConfigId;
  }

  public void setFunctionAutotuneConfigId(Long functionAutotuneConfigId) {
    this.functionAutotuneConfigId = functionAutotuneConfigId;
  }

  public boolean isSpeedUp() {
    return speedUp;
  }

  public void setSpeedUp(boolean speedUp) {
    this.speedUp = speedUp;
  }

  public boolean isSelfKill() {
    return selfKill;
  }

  public void setSelfKill(boolean selfKill) {
    this.selfKill = selfKill;
  }

  public long getLastClonedFunctionId(){return lastClonedFunctionId;}
}
