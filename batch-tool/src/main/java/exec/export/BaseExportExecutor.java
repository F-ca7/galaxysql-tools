/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
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

package exec.export;

import cmd.BaseOperateCommand;
import cmd.ExportCommand;
import com.alibaba.druid.pool.DruidDataSource;
import datasource.DataSourceConfig;
import exec.BaseExecutor;
import model.config.ExportConfig;
import worker.ddl.DdlExportWorker;

public abstract class BaseExportExecutor extends BaseExecutor {

    protected ExportCommand command;
    protected ExportConfig config;

    public BaseExportExecutor(DataSourceConfig dataSourceConfig, DruidDataSource druid,
                              BaseOperateCommand baseCommand) {
        super(dataSourceConfig, druid, baseCommand);
    }

    @Override
    protected void setCommand(BaseOperateCommand baseCommand) {
        this.command = (ExportCommand) baseCommand;
        this.config = command.getExportConfig();
    }

    /**
     * 检查表是否存在
     */
    @Override
    public void preCheck() {
        if (!command.isDbOperation()) {
            checkTableExists(command.getTableNames());
        }
    }

    @Override
    public void execute() {
        Thread ddlThread = null;
        switch (config.getDdlMode()) {
        case NO_DDL:
            exportData();
            break;
        case WITH_DDL:
            ddlThread = exportDDL();
            exportData();
            break;
        case DDL_ONLY:
            ddlThread = exportDDL();
            break;
        }
        if (ddlThread != null) {
            try {
                ddlThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private Thread exportDDL() {
        DdlExportWorker ddlExportWorker;
        if (command.isDbOperation()) {
            ddlExportWorker = new DdlExportWorker(dataSource, command.getDbName());
        } else {
            ddlExportWorker = new DdlExportWorker(dataSource, command.getDbName(), command.getTableNames());
        }
        Thread ddlThread = new Thread(ddlExportWorker);
        ddlThread.start();
        return ddlThread;
    }

    abstract void exportData();
}
