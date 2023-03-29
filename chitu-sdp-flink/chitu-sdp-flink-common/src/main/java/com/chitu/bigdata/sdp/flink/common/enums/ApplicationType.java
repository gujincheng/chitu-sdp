package com.chitu.bigdata.sdp.flink.common.enums;


public enum ApplicationType {

    /**
     * Apache Flink
     */
    APACHE_FLINK(2, "Apache Flink");
    int type;
    String name;

    ApplicationType(int type, String name) {
        this.type = type;
        this.name = name;
    }

    public int getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    public static ApplicationType of(int type) {
        for (ApplicationType appType : ApplicationType.values()) {
            if (appType.getType() == type) {
                return appType;
            }
        }
        return null;
    }
}
