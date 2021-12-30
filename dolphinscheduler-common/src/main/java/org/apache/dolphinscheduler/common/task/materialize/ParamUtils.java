package org.apache.dolphinscheduler.common.task.materialize;

import lombok.Data;

import org.apache.dolphinscheduler.common.enums.DataType;
import org.apache.dolphinscheduler.spi.utils.JSONUtils;

import org.apache.commons.collections4.CollectionUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAdjusters;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParamUtils {

    private static final DateTimeFormatter date_formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private static final DateTimeFormatter time_formatter = DateTimeFormatter.ofPattern("HH:mm:ss");

    private static final DateTimeFormatter timestamp_formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static final Pattern total_replace_Holder_pattern = Pattern.compile("^'\\$\\{(.*)}'$");

    public static Set<String> numericalTypes;

    static {
        Set<String> tmp = new HashSet<>();
        tmp.add("INTEGER");
        tmp.add("LONG");
        tmp.add("FLOAT");
        tmp.add("DOUBLE");
        numericalTypes = Collections.unmodifiableSet(tmp);
    }

    public static DataType convertToDataType(Param param) {
        return param.getArray() ? DataType.valueOf("ARRAY_" + param.getType().toUpperCase(Locale.ROOT))
            : DataType.valueOf(param.getType().toUpperCase(Locale.ROOT));
    }

    public static Object paramValue(Param param) throws Exception {
        if (!param.getArray()) {
            return singleValue(param, param.getParamValues().get(0));
        }
        Set<Object> all = new HashSet<>();
        for (ParamValue paramValue : param.getParamValues()) {
            all.addAll(listValue(param, paramValue));
        }
        return all;
    }

    public static Map<String, Object> paramValues(List<Param> params) throws Exception {
        if (params == null || params.size() == 0) {
            return Collections.emptyMap();
        }
        Map<String, Object> paramValues = new HashMap<>(params.size());
        for (Param param : params) {
            paramValues.put(param.getName(), paramValue(param));
        }
        return paramValues;
    }

    public static List<Object> listValue(Param param, ParamValue paramValue) throws Exception {
        switch (paramValue.getFrom().toUpperCase(Locale.ROOT)) {
            case "SQL_QUERY":
                return listJdbcValue(param, paramValue);
            case "CONSTANT":
                return listConstantValue(param, paramValue);
            case "FUNCTION":
                return functionValue(param, paramValue);
            default:
                throw new IllegalArgumentException("not support param from:" + paramValue.getFrom());
        }
    }

    public static Object singleValue(Param param, ParamValue paramValue) throws Exception {
        switch (paramValue.getFrom().toUpperCase(Locale.ROOT)) {
            case "SQL_QUERY":
                return singleJdbcValue(param, paramValue);
            case "FUNCTION":
                return functionValue(param, paramValue).get(0);
            case "CONSTANT":
                return singleConstantValue(param, paramValue);
            default:
                throw new IllegalArgumentException("not support param from:" + paramValue.getFrom());
        }
    }

    @Data
    private static class FunctionConfig {
        private String type;
        private Integer intervalDays;
        private Integer intervalMonths;
        private Integer intervalYears;
        private String start;
        private String end;
    }

    public static List<Object> functionValue(Param param, ParamValue paramValue) throws Exception {
        FunctionConfig functionConfig = JSONUtils.parseObject(paramValue.getConfig(), FunctionConfig.class);
        Map<String, Object> childValue = paramValues(paramValue.getChildParams());
        switch (functionConfig.getType().toUpperCase(Locale.ROOT)) {
            case "ALL_MONTH_END":
                Matcher matcher = total_replace_Holder_pattern.matcher(functionConfig.getStart());
                if (matcher.find()) {
                    functionConfig.setStart((String) childValue.get(matcher.group(1)));
                }
                matcher = total_replace_Holder_pattern.matcher(functionConfig.getEnd());
                if (matcher.find()) {
                    functionConfig.setEnd((String) childValue.get(matcher.group(1)));
                }
                return convertListObject(allMonthEnd(functionConfig.getStart(), functionConfig.getEnd()));
            case "ALL_MONTH_START":
                matcher = total_replace_Holder_pattern.matcher(functionConfig.getStart());
                if (matcher.find()) {
                    functionConfig.setStart((String) childValue.get(matcher.group(1)));
                }
                matcher = total_replace_Holder_pattern.matcher(functionConfig.getEnd());
                if (matcher.find()) {
                    functionConfig.setEnd((String) childValue.get(matcher.group(1)));
                }
                return convertListObject(allMonthStart(functionConfig.getStart(), functionConfig.getEnd()));
            case "MONTH_END":
                LocalDate localDate = LocalDate.now().with(TemporalAdjusters.lastDayOfMonth());
                return Collections.singletonList(intervalDate(localDate, functionConfig.getIntervalDays(),
                    functionConfig.getIntervalMonths(), functionConfig.getIntervalYears()));
            case "MONTH_START":
                localDate = LocalDate.now().with(TemporalAdjusters.firstDayOfMonth());
                return Collections.singletonList(intervalDate(localDate, functionConfig.getIntervalDays(),
                    functionConfig.getIntervalMonths(), functionConfig.getIntervalYears()));
            case "YEAR_START":
                localDate = LocalDate.now().with(TemporalAdjusters.firstDayOfYear());
                return Collections.singletonList(intervalDate(localDate, functionConfig.getIntervalDays(),
                    functionConfig.getIntervalMonths(), functionConfig.getIntervalYears()));
            case "YEAR_END":
                localDate = LocalDate.now().with(TemporalAdjusters.lastDayOfYear());
                return Collections.singletonList(intervalDate(localDate, functionConfig.getIntervalDays(),
                    functionConfig.getIntervalMonths(), functionConfig.getIntervalYears()));
            case "LAST_MONTH_START":
                localDate = LocalDate.now().minusMonths(1).with(TemporalAdjusters.firstDayOfMonth());
                return Collections.singletonList(intervalDate(localDate, functionConfig.getIntervalDays(),
                    functionConfig.getIntervalMonths(), functionConfig.getIntervalYears()));
            case "LAST_MONTH_END":
                localDate = LocalDate.now().minusMonths(1).with(TemporalAdjusters.lastDayOfMonth());
                return Collections.singletonList(intervalDate(localDate, functionConfig.getIntervalDays(),
                    functionConfig.getIntervalMonths(), functionConfig.getIntervalYears()));
            case "LAST_YEAR_END":
                localDate = LocalDate.now().minusYears(1).with(TemporalAdjusters.lastDayOfYear());
                return Collections.singletonList(intervalDate(localDate, functionConfig.getIntervalDays(),
                    functionConfig.getIntervalMonths(), functionConfig.getIntervalYears()));
            case "LAST_YEAR_START":
                localDate = LocalDate.now().minusYears(1).with(TemporalAdjusters.firstDayOfYear());
                return Collections.singletonList(intervalDate(localDate, functionConfig.getIntervalDays(),
                    functionConfig.getIntervalMonths(), functionConfig.getIntervalYears()));
            case "TODAY":
                localDate = LocalDate.now();
                return Collections.singletonList(intervalDate(localDate, functionConfig.getIntervalDays(),
                    functionConfig.getIntervalMonths(), functionConfig.getIntervalYears()));
            case "YESTERDAY":
                localDate = LocalDate.now().minusDays(1);
                return Collections.singletonList(intervalDate(localDate, functionConfig.getIntervalDays(),
                    functionConfig.getIntervalMonths(), functionConfig.getIntervalYears()));
            case "THE_DAY_BEFORE_YESTERDAY":
                localDate = LocalDate.now().minusDays(2);
                return Collections.singletonList(intervalDate(localDate, functionConfig.getIntervalDays(),
                    functionConfig.getIntervalMonths(), functionConfig.getIntervalYears()));
            default:
                throw new IllegalArgumentException("not support function type:" + functionConfig.getType());
        }
    }

    public static Object singleConstantValue(Param param, ParamValue paramValue) {
        switch (param.getType().toUpperCase(Locale.ROOT)) {
            case "INTEGER":
                return Integer.valueOf(paramValue.getConfig());
            case "LONG":
                return Long.valueOf(paramValue.getConfig());
            case "FLOAT":
                return Float.valueOf(paramValue.getConfig());
            case "DOUBLE":
                return Double.valueOf(paramValue.getConfig());
            default:
                return paramValue.getConfig();
        }
    }

    public static List<Object> listConstantValue(Param param, ParamValue paramValue) {
        switch (param.getType().toUpperCase(Locale.ROOT)) {
            case "INTEGER":
                return convertListObject(JSONUtils.toList(paramValue.getConfig(), Integer.class));
            case "LONG":
                return convertListObject(JSONUtils.toList(paramValue.getConfig(), Long.class));
            case "FLOAT":
                return convertListObject(JSONUtils.toList(paramValue.getConfig(), Float.class));
            case "DOUBLE":
                return convertListObject(JSONUtils.toList(paramValue.getConfig(), Double.class));
            default:
                return convertListObject(JSONUtils.toList(paramValue.getConfig(), String.class));
        }
    }

    public static <T> List<Object> convertListObject(List<T> list) {
        List<Object> list1 = new ArrayList<>(list.size());
        list1.addAll(list);
        return list1;
    }

    public static Object singleJdbcValue(Param param, ParamValue paramValue) throws Exception {
        Map<String, Object> childValue = paramValues(paramValue.getChildParams());
        String sql = paramValue.getConfig();
        sql = replaceSqlHolder(sql, childValue);
        ResultGetFunction getter = convertToDataType(param.getType());
        ReadConfig readConfig = paramValue.getReadConfig();
        String jdbcUrl = jdbcUrl(readConfig);
        try (Connection connection = DriverManager.getConnection(jdbcUrl, readConfig.getUserName(), readConfig.getPassword());
             Statement statement = connection.createStatement()) {
            statement.setQueryTimeout(60);
            ResultSet resultSet = statement.executeQuery(sql);
            if (resultSet.next()) {
                return String.valueOf(getter.getFirst(resultSet));
            }
            resultSet.close();
        }
        return null;
    }

    public static List<Object> listJdbcValue(Param param, ParamValue paramValue) throws Exception {
        Map<String, Object> childValue = paramValues(paramValue.getChildParams());
        String sql = paramValue.getConfig();
        sql = replaceSqlHolder(sql, childValue);
        ResultGetFunction getter = convertToDataType(param.getType());
        ReadConfig readConfig = paramValue.getReadConfig();
        String jdbcUrl = jdbcUrl(readConfig);
        List<Object> result = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection(jdbcUrl, readConfig.getUserName(), readConfig.getPassword());
             Statement statement = connection.createStatement()) {
            statement.setQueryTimeout(60);
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                result.add(getter.getFirst(resultSet));
            }
            resultSet.close();
        }
        return result;
    }


    public static String jdbcUrl(ReadConfig readConfig) {
        return  "jdbc:" + readConfig.getType().toLowerCase(Locale.ROOT) + "//" + readConfig.getIp() + ":" + readConfig.getPort() + "/"
            + readConfig.getSchema();
    }

    private static ResultGetFunction convertToDataType(String type) {
        switch (type.toUpperCase(Locale.ROOT)) {
            case "INTEGER":
                return resultSet -> resultSet.getInt(1);
            case "LONG":
                return resultSet -> resultSet.getLong(1);
            case "FLOAT":
                return resultSet -> resultSet.getFloat(1);
            case "DOUBLE":
                return resultSet -> resultSet.getDouble(1);
//            case "DATE":
//                return resultSet -> resultSet.getDate(1);
//            case "TIME":
//                return resultSet -> resultSet.getTime(1);
//            case "TIMESTAMP":
//                return resultSet -> resultSet.getTimestamp(1);
//            case "BOOLEAN":
//                return resultSet -> resultSet.getBoolean(1);
            default:
                return resultSet -> resultSet.getString(1);
        }
    }

    @FunctionalInterface
    private interface ResultGetFunction {
        Object getFirst(ResultSet resultSet) throws SQLException;
    }

    public static boolean numerical(String type) {
        return numericalTypes.contains(type.toUpperCase(Locale.ROOT));
    }

    /**
     * str -> select id from table_1 where name='${a}' and id = '${b}' and city in ('${c}')
     * holderValues -> a:"tom" b:4 c:["杭州","北京","上海"]
     * return -> select id from table_1 where name="to\"m" and id = 1 and city in ("杭州","北京")
     *
     * ------ code -----
     *
     *        Map<String, Object> holderValues = new HashMap<>();
     *         holderValues.put("a", "to\"m");
     *         holderValues.put("b", 1);
     *         List<String> list = new ArrayList<>();
     *         list.add("杭州");
     *         list.add("北京");
     *         holderValues.put("c", list);
     *         System.out.println(replaceSqlHolder("select id from table_1 where name='${a}' and id = '${b}' and city in ('${c}')", holderValues));
     *
     *
     *
     *
     *
     * 字符和日期类型,替换时会添加上引号
     * 列表类型会逗号隔开: 1,2,3 / "杭州","北京","上海"
     * @param sql
     * @param holderValues
     * @return
     */
    public static String replaceSqlHolder(String sql, Map<String, Object> holderValues) {
        if (sql == null || holderValues == null || holderValues.size() == 0) {
            return sql;
        }
        for (Map.Entry<String, Object> entry : holderValues.entrySet()) {
            if (entry.getValue() instanceof List) {
                String replaceValue = JSONUtils.toJsonString(entry.getValue());
                sql = sql.replace("'${" + entry.getKey() + "}'", replaceValue.substring(1, replaceValue.length()-1));
            } else {
                sql = sql.replace("'${" + entry.getKey() + "}'", JSONUtils.toJsonString(entry.getValue()));
            }
        }
        return sql;
    }

    public static List<String> allMonthEnd(String startDate, String endDate) {
        LocalDate start = LocalDate.parse(startDate, date_formatter).with(TemporalAdjusters.lastDayOfMonth());
        LocalDate end = LocalDate.parse(endDate, date_formatter);
        List<String> allMonthEnd = new ArrayList<>();
        while (!end.isBefore(start)) {
            allMonthEnd.add(date_formatter.format(start));
            start = start.plusMonths(1).with(TemporalAdjusters.lastDayOfMonth());
        }
        return allMonthEnd;
    }

    public static List<String> allMonthStart(String startDate, String endDate) {
        LocalDate start = LocalDate.parse(startDate, date_formatter);
        LocalDate end = LocalDate.parse(endDate, date_formatter).with(TemporalAdjusters.firstDayOfMonth());
        List<String> allMonthStart = new ArrayList<>();
        while (!end.isBefore(start)) {
            allMonthStart.add(date_formatter.format(end));
            end = end.minusMonths(1).with(TemporalAdjusters.firstDayOfMonth());
        }
        return allMonthStart;
    }

    public static String intervalDate(LocalDate localDate, Integer intervalDays, Integer intervalMonths, Integer intervalYears) {
        if (intervalDays != null) {
            localDate = localDate.plusDays(intervalDays);
        }
        if (intervalMonths != null) {
            localDate = localDate.plusMonths(intervalMonths);
        }
        if (intervalYears != null) {
            localDate = localDate.plusYears(intervalYears);
        }
        return date_formatter.format(localDate);
    }



    public static void main(String[] args) {
        System.out.println(allMonthStart("2021-01-02", "2021-03-01"));
    }
}
