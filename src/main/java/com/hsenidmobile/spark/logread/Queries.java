package com.hsenidmobile.spark.logread;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.util.List;

public class Queries {

    Config conf = ConfigFactory.parseResources("TypeSafeConfig.conf");

    public String selectString = conf.getString("query.selectedFields");
    public String filterString = conf.getString("query.whereClause");
    public String tableName = conf.getString("output.tableName");
    public String viewTableName = conf.getString("output.temporyView");
    public String createTableColumnList = conf.getString("output.createTableQuery");
    public String tableColumns = conf.getString("output.columnNames");
    public int numberOfColumns = conf.getInt("output.numberOfColumns");
    private List<String> groupingFields = conf.getStringList("query.groupingFields");

    public String getTableColumns() {
        return tableColumns;
    }

    public int getNumberOfColumns() {
        return numberOfColumns;
    }

    public String getFilterStatement() {
        return filterString;
    }

    public int getNumberOfGroupingFields() {
        return groupingFields.size();
    }

    public String getGroupingField(int listNumber){
        return groupingFields.get(listNumber -1);
    }

    public String groupingQuery() {
        String query = "SELECT time_stamp,app_id,CAST(SUM(CAST (count AS int)) AS String) AS count FROM "+tableName+" GROUP BY time_stamp, app_id ";
        return query;
    }

    public String getCreateTableQuery() {
        String query = "CREATE TABLE IF NOT EXISTS "+tableName +"( " +createTableColumnList +" )";
        return query;
    }

    public String getTruncateTableQuery() {
        String query = "TRUNCATE TABLE " +tableName ;
        return query;
    }

    public String insertDataFromTempViewQuery(String tempViewName) {
        String query = "INSERT INTO " + tableName + " SELECT " +tableColumns + " FROM " + tempViewName ;
        return query;
    }

    public String getCreateViewQuery() {
        String query = "CREATE TABLE IF NOT EXISTS "+viewTableName +"( " +createTableColumnList +" )";
        return query;
    }

    public String getInsertViewQuery(String tempViewName) {
        String query = "INSERT INTO " + viewTableName + " SELECT " +tableColumns + " FROM " + tempViewName ;
        return query;
    }

    public String getDropViewQuery() {
        String query = "DROP TABLE " + viewTableName;
        return query;
    }

    public String getPrintTableQuery() {
        String query = "SELECT * FROM " +tableName;
        return query;
    }


}
