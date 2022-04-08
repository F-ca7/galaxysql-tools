package worker.ddl;

import model.config.ConfigConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.IOUtil;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * 直接通过source导入库表
 */
public class DdlImportWorker implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(DdlExportWorker.class);
    private final List<String> filepaths;
    private final DataSource druid;

    public DdlImportWorker(List<String> dbOrTbNames, DataSource druid) {
        this.druid = druid;
        this.filepaths = new ArrayList<>();
        for (String name : dbOrTbNames) {
            String filename = name + ConfigConstant.DDL_FILE_SUFFIX;
            File file = new File(filename);
            if (!file.exists() || !file.isFile()) {
                throw new IllegalStateException("File " + filename + " does not exist");
            }
            this.filepaths.add(file.getAbsolutePath());
        }
    }

    @Override
    public void run() {
        BufferedReader reader = null;
        try (Connection conn = druid.getConnection();
            Statement stmt = conn.createStatement()) {

            StringBuilder sqlStringBuilder = new StringBuilder(100);
            String line = null;
            for (String filepath : filepaths) {
                reader = new BufferedReader(new FileReader(filepath));
                while ((line = reader.readLine()) != null) {
                    if (line.startsWith("--")) {
                        continue;
                    }
                    if (!line.endsWith(";")) {
                        sqlStringBuilder.append(line).append("\n");
                    } else {
                        sqlStringBuilder.append(line);
                        String sql = sqlStringBuilder.toString();
                        stmt.execute(sql);
                        sqlStringBuilder.setLength(0);
                    }
                }
                sqlStringBuilder.setLength(0);
                IOUtil.close(reader);
            }
        } catch (IOException | SQLException e) {
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        } finally {
            IOUtil.close(reader);
        }
        logger.info("DDL语句导入完毕");
    }
}
