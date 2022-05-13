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
import cmd.DeleteCommand;
import com.alibaba.druid.pool.DruidDataSource;
import datasource.DataSourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import worker.delete.DeleteConsumer;
import worker.delete.DeleteInConsumer;
import worker.delete.ShardedDeleteInConsumer;

public class DeleteExecutor extends WriteDbExecutor {
    private static final Logger logger = LoggerFactory.getLogger(DeleteExecutor.class);

    private DeleteCommand command;

    public DeleteExecutor(DataSourceConfig dataSourceConfig,
                          DruidDataSource druid,
                          BaseOperateCommand baseCommand) {
        super(dataSourceConfig, druid, baseCommand);
    }

    @Override
    protected void setCommand(BaseOperateCommand baseCommand) {
        this.command = (DeleteCommand) baseCommand;
    }

    @Override
    public void execute() {
        configurePkList();
        for (String tableName : tableNames) {
            if (command.isShardingEnabled()) {
                doShardingDelete(tableName);
            } else {
                doDefaultDelete(tableName);
            }
            logger.info("删除 {} 数据完成", tableName);
        }
    }

    private void doDefaultDelete(String tableName) {
        if (consumerExecutionContext.isWhereInEnabled()) {
            // 使用delete ... in (...)
            configureFieldMetaInfo();
            configureCommonContextAndRun(DeleteInConsumer.class,
                producerExecutionContext, consumerExecutionContext, tableName);
        } else {
            configurePkList();
            configureCommonContextAndRun(DeleteConsumer.class,
                producerExecutionContext, consumerExecutionContext, tableName);
        }
    }

    private void doShardingDelete(String tableName) {
        configureFieldMetaInfo();
        configureTopology();
        configurePartitionKey();
        configureCommonContextAndRun(ShardedDeleteInConsumer.class,
            producerExecutionContext, consumerExecutionContext, tableName);
    }
}
