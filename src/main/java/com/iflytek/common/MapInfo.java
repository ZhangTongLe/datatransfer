package com.iflytek.common;

public class MapInfo {
    /**
     * 源信息
     */
    private String source;
    /**
     * 目标信息
     */
    private String dest;

    private String sourceType;

    public String getSourceType() {
        return sourceType;
    }

    public void setSourceType(String sourceType) {
        this.sourceType = sourceType;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getDest() {
        return dest;
    }

    public void setDest(String dest) {
        this.dest = dest;
    }

    public MapInfo(){

    }

    public MapInfo(String source,String dest,String sourceType){
        this.source = source;
        this.dest = dest;
        this.sourceType  = sourceType;
    }

    @Override
    public String toString() {
        return "[" + source + "==>" +
                 dest  +
                ", sourceType='" + sourceType +
                ']';
    }
}
