package org.apache.dolphinscheduler.common.task.materialize;

import lombok.Data;

import org.apache.dolphinscheduler.spi.utils.JSONUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class ParamUtils {

    public static String paramValue(Param param) throws Exception {
        switch (param.getFrom().toUpperCase(Locale.ROOT)) {
            case "SQL_QUERY":
                return jdbcValue(param);
            case "FUNCTION":
                return functionValue(param);
            case "CONSTANT":
                return constantValue(param);
            default:
                throw new IllegalArgumentException("not support param from:" + param.getFrom());
        }
    }

    @Data
    private static class FunctionConfig {
        private String type;
        private String start;
        private String end;
    }

    public static String functionValue(Param param) {
        FunctionConfig functionConfig = JSONUtils.parseObject(param.getConfig(), FunctionConfig.class);
        switch (functionConfig.getType().toUpperCase(Locale.ROOT)) {
            case "ALL_MONTH_END":

                return null;
            default:
                throw new IllegalArgumentException("not support function type:" + functionConfig.getType());
        }
    }

    public static String constantValue(Param param) {
        return param.getConfig();
    }

    public static String jdbcValue(Param param) throws SQLException {
        String type = param.getType();
        int index = type.indexOf("_");
        boolean array = false;
        if (index > 0) {
            array = "array".equalsIgnoreCase(type.substring(0, index));
            type = type.substring(index + 1);
        }
        ResultGetFunction get = parse(type);

        ReadConfig readConfig = param.getReadConfig();
        String jdbcUrl = jdbcUrl(readConfig);

        if (array) {
            List<Object> result = new ArrayList<>();
            try (Connection connection = DriverManager.getConnection(jdbcUrl, readConfig.getUserName(), readConfig.getPassword());
                 Statement statement = connection.createStatement()) {
                statement.setQueryTimeout(60);
                ResultSet resultSet = statement.executeQuery(param.getConfig());
                while (resultSet.next()) {
                    result.add(get.get(resultSet));
                }
                resultSet.close();
            }
            return JSONUtils.toJsonString(result);
        } else {
            try (Connection connection = DriverManager.getConnection(jdbcUrl, readConfig.getUserName(), readConfig.getPassword());
                 Statement statement = connection.createStatement()) {
                statement.setQueryTimeout(60);
                ResultSet resultSet = statement.executeQuery(param.getConfig());
                if (resultSet.next()) {
                    return String.valueOf(get.get(resultSet));
                }
                resultSet.close();
            }
            return null;
        }
    }

    public static String jdbcUrl(ReadConfig readConfig) {
        return  "jdbc:" + readConfig.getType().toLowerCase(Locale.ROOT) + "//" + readConfig.getIp() + ":" + readConfig.getPort() + "/"
            + readConfig.getSchema();
    }

    private static ResultGetFunction parse(String type) {
        switch (type.toUpperCase(Locale.ROOT)) {
            case "INTEGER":
                return resultSet -> resultSet.getInt(1);
            case "LONG":
                return resultSet -> resultSet.getLong(1);
            case "FLOAT":
                return resultSet -> resultSet.getFloat(1);
            case "DOUBLE":
                return resultSet -> resultSet.getDouble(1);
            case "DATE":
                return resultSet -> resultSet.getDate(1);
            case "TIME":
                return resultSet -> resultSet.getTime(1);
            case "TIMESTAMP":
                return resultSet -> resultSet.getTimestamp(1);
            case "BOOLEAN":
                return resultSet -> resultSet.getBoolean(1);
            default:
                return resultSet -> resultSet.getString(1);
        }
    }

    @FunctionalInterface
    private interface ResultGetFunction {
        Object get(ResultSet resultSet) throws SQLException;
    }


    public static String replaceHolder(String value, Map<String, String> holderValues) {
        if (value == null || holderValues == null) {
            return value;
        }
        for (Map.Entry<String, String> entry : holderValues.entrySet()) {
            value = value.replace("${" + entry.getKey() + "}", entry.getValue());
        }
        return value;
    }
}
