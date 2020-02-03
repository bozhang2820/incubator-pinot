/**
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
package org.apache.pinot.tools.admin.command;

import org.apache.commons.lang.StringUtils;
import org.apache.pinot.core.realtime.impl.kafka.KafkaStarterUtils;
import org.apache.pinot.grigio.common.keyValueStore.RocksDBConfig;
import org.apache.pinot.grigio.common.storageProvider.UpdateLogStorageProvider;
import org.apache.pinot.grigio.keyCoordinator.starter.KeyCoordinatorConf;
import org.apache.pinot.grigio.keyCoordinator.starter.KeyCoordinatorStarter;
import org.apache.pinot.tools.Command;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;


/**
 * Class to implement StartCoordinator command.
 *
 */
public class StartCoordinatorCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(StartCoordinatorCommand.class);

  @Option(name = "-configFileName", required = true, metaVar = "<Config File Name>", usage = "Broker Starter Config file.")
  private String _configFileName;

  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h", "--help"}, usage = "Print this message.")
  private boolean _help = false;

  @Option(name = "-StorageDir", required = false, metaVar = "<string>", usage = "Path to directory containing rocksDB " +
      "and update logs.")
  private String _storageDir = TMP_DIR + "pinotKCStorage" + File.separator;

  @Option(name = "-tableName", required = false, metaVar = "<string>", usage = "tableName")
  private String _tableName = "";

  private boolean _shouldCreateKafkaTopic = true;
  private String _kcUpdateLogPath;
  private String _kcRocksDBPath;

  @Override
  public boolean getHelp() {
    return _help;
  }

  public StartCoordinatorCommand setConfigFileName(String configFileName) {
    _configFileName = configFileName;
    return this;
  }

  public StartCoordinatorCommand setShouldCreateKafkaTopic(boolean _shouldCreateKafkaTopic) {
    this._shouldCreateKafkaTopic = _shouldCreateKafkaTopic;
    return this;
  }

  @Override
  public String toString() {
    return ("StartCoordinator -configFileName " + _configFileName);
  }

  @Override
  public String getName() {
    return "StartCoordinator";
  }

  @Override
  public void cleanup() {

  }

  @Override
  public String description() {
    return "Start the Pinot Coordinator process.";
  }

  private void ensureDirectoryCreated(KeyCoordinatorConf conf) {
    _kcUpdateLogPath = _storageDir + "kcUpdateLogs";
    _kcRocksDBPath = _storageDir + "kcRocksDB";
    File kcRocksDBDir = new File(_kcRocksDBPath);
    File kcUpdateLogDir = new File(_kcUpdateLogPath);
    kcRocksDBDir.mkdir();
    kcRocksDBDir.deleteOnExit();
    kcUpdateLogDir.mkdir();
    kcUpdateLogDir.deleteOnExit();

    LOGGER.info("starting kc with update log path {}, kv store path {}", _kcUpdateLogPath, _kcRocksDBPath);
    conf.addProperty(String.join(".", KeyCoordinatorConf.KEY_COORDINATOR_KV_STORE, RocksDBConfig.DATABASE_DIR),
        _kcRocksDBPath);
    conf.addProperty(String.join(".", KeyCoordinatorConf.STORAGE_PROVIDER_CONFIG,
        UpdateLogStorageProvider.BASE_PATH_CONF_KEY), _kcUpdateLogPath);
  }

  private void ensureKafkaTopicCreated(KeyCoordinatorConf conf) {
    final String topic = conf.getKeyCoordinatorMessageTopic();
    final int partitionCount = conf.getKeyCoordinatorMessagePartitionCount();
    KafkaStarterUtils.createTopic(topic, "localhost:2181", partitionCount);

    if (StringUtils.isNotEmpty(_tableName)) {
      KafkaStarterUtils.createTopic(conf.getTopicPrefix() + _tableName, "localhost:2181",
          partitionCount);
    }
  }

  @Override
  public boolean execute()
      throws Exception {
    System.out.println("start to execute coordinator");
    try {
      if (StringUtils.isEmpty(_configFileName)) {
        LOGGER.error("config file parameter is empty string");
        return false;
      }
      File configFile = new File(_configFileName);
      if (!configFile.exists()) {
        LOGGER.error("config file {} is not found", _configFileName);
        return false;
      }
      KeyCoordinatorConf conf = new KeyCoordinatorConf(new File(_configFileName));
      ensureDirectoryCreated(conf);
      if (_shouldCreateKafkaTopic) {
        ensureKafkaTopicCreated(conf);
      }

      LOGGER.info("Executing command: " + toString());
      KeyCoordinatorStarter starter = KeyCoordinatorStarter.startDefault(conf);
      starter.startManually();
      return true;
    } catch (Exception e) {
      LOGGER.error("Caught exception while starting Pinot server, exiting.", e);
      System.exit(-1);
      return false;
    }
  }
}
