package com.iflytek.common;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * 数据库表信息
 */
public class TableColumn {
    /**
     * 名称
     */
    private String name;
    /**
     * 类型
     */
    private String type;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public TableColumn(){

    }

    public TableColumn(String name,String type){
        this.name = name;
        this.type = type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableColumn that = (TableColumn) o;
        if (name == that.name) return true;
        if (name != null){
            if ( name.equalsIgnoreCase(that.name)){
                return true;
            }
            if (name.equalsIgnoreCase(that.name.replaceAll("KONWLEDGE|konwledge", "KNOWLEDGE"))){
                return true;
            }
            if (that.name.equalsIgnoreCase(name.replaceAll("KONWLEDGE|konwledge", "KNOWLEDGE"))){
                return true;
            }
            if (name.equalsIgnoreCase("HERARCHY") && that.name.equalsIgnoreCase("HIERARCHY")){
                return true;
            }
            if (name.equalsIgnoreCase("HIERARCHY") && that.name.equalsIgnoreCase("HERARCHY")){
                return true;
            }
        }
        return false;

    }

    @Override
    public int hashCode() {

        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return "{" +
                "name='" + name + '\'' +
                ", type='" + type + '\'' +
                '}';
    }

    public static List<String> subtract(List<TableColumn> list,List<String> linked){
        List<String> tmp = new ArrayList<>();
        list.forEach(x->tmp.add(x.getName()));
        tmp.removeAll(linked);
        return tmp;
    }
 }
