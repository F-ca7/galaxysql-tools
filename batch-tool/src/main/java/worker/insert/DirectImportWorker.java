package worker.insert;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import model.config.FileRecord;
import model.db.FieldMetaInfo;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.IOUtil;

import javax.sql.DataSource;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.IntStream;

/**
 * 按行读取并且按行
 * 支持断点记录与恢复
 * 性能较差
 */
public class DirectImportWorker implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(DirectImportWorker.class);

    private final DataSource dataSource;
    private final char sep;
    private final List<FileRecord> fileRecords;
    private final String tableName;
    private final Charset charset;
    private final List<FieldMetaInfo> fieldMetaInfoList;

    public DirectImportWorker(DataSource dataSource, String sep,
                              Charset charset,
                              List<FileRecord> fileRecords, String tableName,
                              List<FieldMetaInfo> fieldMetaInfoList) {
        if (sep.length() != 1) {
            throw new RuntimeException("Only allows single char separator in safe mode");
        }
        this.sep = sep.charAt(0);
        this.fileRecords = fileRecords;
        this.dataSource = dataSource;
        this.charset = charset;
        this.fieldMetaInfoList = fieldMetaInfoList;
        this.tableName = tableName;
    }

    @Override
    public void run() {
        CSVParser parser = new CSVParserBuilder().withSeparator(sep).build();
        int curLine = 0;
        String curFile = null;
        String[] quesMarks = IntStream.range(0, fieldMetaInfoList.size())
            .mapToObj(c -> "?")
            .toArray(String[]::new);
        String placeholders = StringUtils.join(quesMarks, ",");
        String prepareSql = String.format("INSERT IGNORE INTO %s VALUES (%s)", tableName, placeholders);
        try (Connection conn = dataSource.getConnection();
            PreparedStatement stmt = conn.prepareStatement(prepareSql)) {
            for (int i = 0; i < fileRecords.size(); i++) {
                String filePath = fileRecords.get(i).getFilePath();
                int startLine = fileRecords.get(i).getStartLine();
                curFile = filePath;
                curLine = startLine;
                int importedLines = 0;
                CSVReader reader = new CSVReaderBuilder(new InputStreamReader(
                    new FileInputStream(filePath), charset))
                    .withCSVParser(parser).build();
                reader.skip(startLine - 1);
                for (String[] values; (values = reader.readNext()) != null; ) {
                    for (int i1 = 0; i1 < values.length; i1++) {
                        stmt.setObject(i1 + 1, values[i1]);
                    }
                    stmt.executeUpdate();
                    curLine++;
                    importedLines++;
                    if (importedLines % 1000 == 0) {
                        logger.info("File {} import {} line", curFile, importedLines);
                    }
                }
                IOUtil.close(reader);
                logger.info("File {} import successfully!", curFile);
            }
        } catch (SQLException | IOException e) {
            e.printStackTrace();
            logger.error("Failed in file {} at line {}", curFile, curLine);
            throw new RuntimeException(e);
        }
    }


}
