package model.config;

import java.util.List;
import java.util.stream.Collectors;

public class FileLineRecord {
    private final String filePath;
    /**
     * 从第1行开始
     */
    private final int startLine;

    public FileLineRecord(String filePath) {
        this.filePath = filePath;
        this.startLine = 1;
    }

    public FileLineRecord(String filePath, int startLine) {
        if (startLine <= 0) {
            throw new IllegalArgumentException("Start line starts from 1");
        }
        this.filePath = filePath;
        this.startLine = startLine;
    }

    public static List<FileLineRecord> fromFilePaths(List<String> filePaths) {
        return filePaths.stream().map(FileLineRecord::new)
            .collect(Collectors.toList());
    }

    public String getFilePath() {
        return filePath;
    }

    @Override
    public String toString() {
        return "FileLineRecord{" +
            "filePath='" + filePath + '\'' +
            ", startLine=" + startLine +
            '}';
    }

    public int getStartLine() {
        return startLine;
    }
}
