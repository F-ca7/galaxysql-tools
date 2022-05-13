package worker.factory;

import cmd.ExportCommand;
import exception.DatabaseException;
import model.config.ExportConfig;
import model.db.TableFieldMetaInfo;
import model.db.TableTopology;
import model.encrypt.Cipher;
import util.DbUtil;
import util.FileUtil;
import worker.export.DirectExportWorker;
import worker.export.order.DirectOrderExportWorker;

import javax.sql.DataSource;
import java.sql.SQLException;

public class ExportWorkerFactory {

    public static DirectExportWorker buildDefaultDirectExportWorker(DataSource druid,
                                                                    TableTopology topology,
                                                                    TableFieldMetaInfo tableFieldMetaInfo,
                                                                    String filename,
                                                                    ExportConfig config) {

        DirectExportWorker directExportWorker;
        switch (config.getExportWay()) {
        case MAX_LINE_NUM_IN_SINGLE_FILE:
            directExportWorker = new DirectExportWorker(druid,
                topology, tableFieldMetaInfo,
                config.getLimitNum(),
                filename,
                config.getSeparator(), config.isWithHeader(),
                config.getQuoteEncloseMode(), config.getCompressMode(), config.getFileFormat(), config.getCharset());
            break;
        case DEFAULT:
            directExportWorker = new DirectExportWorker(druid,
                topology, tableFieldMetaInfo,
                filename,
                config.getSeparator(), config.isWithHeader(),
                config.getQuoteEncloseMode(), config.getCompressMode(), config.getFileFormat(), config.getCharset());
            break;
        case FIXED_FILE_NUM:
        default:
            throw new UnsupportedOperationException("Do not support direct export when fixed file num");
        }
        directExportWorker.setWhereCondition(config.getWhereCondition());
        directExportWorker.setCipher(Cipher.getCipher(config.getEncryptionConfig(), true));
        return directExportWorker;
    }

    public static DirectOrderExportWorker buildDirectOrderExportWorker(DataSource druid,
                                                                       TableFieldMetaInfo tableFieldMetaInfo,
                                                                       ExportCommand command,
                                                                       String tableName) {
        ExportConfig config = command.getExportConfig();
        String filePathPrefix = FileUtil.getFilePathPrefix(config.getPath(),
            config.getFilenamePrefix(), tableName);
        int maxLine = 0;
        switch (config.getExportWay()) {
        case MAX_LINE_NUM_IN_SINGLE_FILE:
            maxLine = config.getLimitNum();
            break;
        case FIXED_FILE_NUM:
            // 固定文件数的情况 先拿到全部的行数
            double totalRowCount;
            try {
                totalRowCount = DbUtil.getTableRowCount(druid.getConnection(), tableName);
            } catch (DatabaseException | SQLException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
            int fileNum = config.getLimitNum();
            maxLine = (int) Math.ceil(totalRowCount / fileNum);
            break;
        case DEFAULT:
        default:
            break;
        }
        return new DirectOrderExportWorker(druid, filePathPrefix,
                tableFieldMetaInfo,
                tableName, config.getOrderByColumnNameList(), maxLine,
                config.getSeparator().getBytes(),
                config.isAscending());
    }
}
