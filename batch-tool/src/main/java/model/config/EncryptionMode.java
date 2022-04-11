package model.config;

public enum EncryptionMode {

    NONE(true),
    CAESAR(true),     // naive Caesar encryption
    AES_CBC(false);    // AES/CBC/PKCS5Padding

    private final boolean supportStreamingBit;

    EncryptionMode(boolean supportStreamingBit) {
        this.supportStreamingBit = supportStreamingBit;
    }

    static EncryptionMode fromString(String encryptionMode) {
        switch (encryptionMode.toUpperCase()) {
        case "NONE":
            return NONE;
        case "DEFAULT": // TODO fix default option
        case "CAESAR":
            return CAESAR;
        case "AES":
        case "AES-CBC":
            return AES_CBC;
        default:
            throw new IllegalArgumentException("Unrecognized encryption mode: " + encryptionMode);
        }
    }

    public boolean isSupportStreamingBit() {
        return supportStreamingBit;
    }
}
