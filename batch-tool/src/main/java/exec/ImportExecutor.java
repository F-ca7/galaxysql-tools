/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package exec;

import cmd.BaseOperateCommand;
import cmd.ImportCommand;
import com.alibaba.druid.pool.DruidDataSource;
import datasource.DataSourceConfig;
import exception.DatabaseException;
import model.config.ConfigConstant;
import model.config.DdlMode;
import model.config.QuoteEncloseMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.DbUtil;
import worker.ddl.DdlImportWorker;
import worker.insert.ImportConsumer;
import worker.insert.ProcessOnlyImportConsumer;
import worker.insert.ShardedImportConsumer;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

public class ImportExecutor extends WriteDbExecutor {
    private static final Logger logger = LoggerFactory.getLogger(ImportExecutor.class);

    private ImportCommand command;

    public ImportExecutor(DataSourceConfig dataSourceConfig,
                          DruidDataSource druid,
                          BaseOperateCommand baseCommand) {
        super(dataSourceConfig, druid, baseCommand);
    }

    @Override
    protected void setCommand(BaseOperateCommand baseCommand) {
        this.command = (ImportCommand) baseCommand;
    }

    @Override
    public void preCheck() {
        if (producerExecutionContext.getDdlMode() != DdlMode.NO_DDL) {
            if (command.isDbOperation()) {
                checkDbNotExist(command.getDbName());
            } else {
                checkTableNotExist(command.getTableNames());
            }
        } else {
            if (command.isDbOperation()) {
                throw new UnsupportedOperationException("Don't support import database now");
            } else {
                checkTableExists(command.getTableNames());
            }
        }
    }

    private void checkDbNotExist(String dbName) {
        if (ConfigConstant.DEFAULT_SCHEMA_NAME.equalsIgnoreCase(dbName)) {
            return;
        }
        try (Connection conn = dataSource.getConnection()) {
            if (DbUtil.checkDatabaseExists(conn, dbName)) {
               throw new RuntimeException(String.format("Database [%s] already exists, cannot import with ddl",
                   dbName));
            }
        } catch (SQLException | DatabaseException e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
    }

    private void checkTableNotExist(List<String> tableNames) {
        for (String tableName : tableNames) {
            try (Connection conn = dataSource.getConnection()) {
                if (DbUtil.checkTableExists(conn, tableName)) {
                    throw new RuntimeException(String.format("Table [%s] already exists, cannot import with ddl",
                        tableNames));
                }
            } catch (SQLException | DatabaseException e) {
                e.printStackTrace();
                throw new RuntimeException(e.getMessage());
            }
        }
    }

    @Override
    public void execute() {
        switch (producerExecutionContext.getDdlMode()) {
        case WITH_DDL:
            handleDDL();
            break;
        case DDL_ONLY:
            handleDDL();
            return;
        case NO_DDL:
            break;
        default:
            throw new UnsupportedOperationException("DDL mode is not supported: " +
                producerExecutionContext.getDdlMode());
        }
        configureFieldMetaInfo();
        // 决定是否要分片
        if (command.isShardingEnabled()) {
            doShardingImport();
        } else {
            doDefaultImport();
        }

        logger.info("导入数据到 {} 完成", tableNames);
    }

    /**
     * 同步导入建库建表语句
     */
    private void handleDDL() {
        DdlImportWorker ddlImportWorker;
        if (command.isDbOperation()) {
            if (producerExecutionContext.getFilePathList().size() != 1) {
                throw new UnsupportedOperationException("Import database DDL only support one ddl file now!");
            }
            ddlImportWorker = new DdlImportWorker(producerExecutionContext.getFilePathList().get(0), dataSource);
        } else {
            ddlImportWorker = new DdlImportWorker(command.getTableNames(), dataSource);
        }

        Thread ddlThread = new Thread(ddlImportWorker);
        ddlThread.start();
        try {
            ddlThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void doDefaultImport() {
        if (consumerExecutionContext.isReadProcessFileOnly()) {
            // 测试读取文件的性能
            configureCommonContextAndRun(ProcessOnlyImportConsumer.class,
                producerExecutionContext, consumerExecutionContext);
        } else {
            configureCommonContextAndRun(ImportConsumer.class,
                producerExecutionContext, consumerExecutionContext,
                producerExecutionContext.getQuoteEncloseMode() != QuoteEncloseMode.FORCE);
        }
    }

    private void doShardingImport() {
        configurePartitionKey();
        configureTopology();

        configureCommonContextAndRun(ShardedImportConsumer.class,
            producerExecutionContext, consumerExecutionContext,
            producerExecutionContext.getQuoteEncloseMode() != QuoteEncloseMode.FORCE);
    }
}
