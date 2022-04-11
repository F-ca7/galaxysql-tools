package model.config;

public enum FileFormat {
    NONE("", true),
    TXT(".txt", true),
    CSV(".csv", true),
    XLSX(".xlsx", false);

    private final String suffix;

    /**
     * 支持按定长块读写
     */
    private final boolean supportBlock;

    FileFormat(String suffix, boolean supportBlock) {
        this.suffix = suffix;
        this.supportBlock = supportBlock;
    }

    public static FileFormat fromString(String compressMode) {
        // NONE / TXT / CSV / XLXS
        switch (compressMode.toUpperCase()) {
        case "NONE":
            return NONE;
        case "TXT":
            return TXT;
        case "CSV":
            return CSV;
        default:
            throw new IllegalArgumentException("Unrecognized file format: " + compressMode);
        }
    }

    public String getSuffix() {
        return suffix;
    }

    public boolean isSupportBlock() {
        return supportBlock;
    }

}
