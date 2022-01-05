package org.apache.dolphinscheduler.common.task.materialize;

import org.apache.dolphinscheduler.common.enums.DataType;
import org.apache.dolphinscheduler.spi.utils.JSONUtils;

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
import java.util.regex.Pattern;

import lombok.Data;

public class ParamUtils {

    private static final DateTimeFormatter date_formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private static final DateTimeFormatter datetime_formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static final Pattern total_replace_Holder_pattern = Pattern.compile("^'\\$\\{(.*)}'$");

    public static Set<String> numericalTypes;

    static {
        Set<String> tmp = new HashSet<>();
        tmp.add(ParamTypeEnum.INTEGER.name());
        tmp.add(ParamTypeEnum.REAL.name());
        numericalTypes = Collections.unmodifiableSet(tmp);
    }

    public static DataType convertToDataType(Param param) {
        ParamTypeEnum paramTypeEnum = ParamTypeEnum.valueOf(param.getType().toUpperCase(Locale.ROOT));
        return param.getArray() ? DataType.valueOf("ARRAY_" + paramTypeEnum.getDataType().name())
            : paramTypeEnum.getDataType();
    }

    public static Object paramValue(Map<String, String> context, Param param) throws Exception {
        if (!param.getArray()) {
            return singleValue(context, param, param.getParamValues().get(0));
        }
        Set<Object> all = new HashSet<>();
        for (ParamValue paramValue : param.getParamValues()) {
            all.addAll(listValue(context, param, paramValue));
        }
        return all;
    }

    public static Map<String, Object> paramValues(Map<String, String> context, List<Param> params) throws Exception {
        if (params == null || params.size() == 0) {
            return Collections.emptyMap();
        }
        Map<String, Object> paramValues = new HashMap<>(params.size());
        for (Param param : params) {
            paramValues.put(param.getName(), paramValue(context, param));
        }
        return paramValues;
    }

    public static List<Object> listValue(Map<String, String> context, Param param, ParamValue paramValue) throws Exception {
        switch (paramValue.getFrom().toUpperCase(Locale.ROOT)) {
            case "SQL_QUERY":
                return listJdbcValue(context, param, paramValue);
            case "CONSTANT":
                return listConstantValue(context, param, paramValue);
            case "FUNCTION":
                return functionValue(context, param, paramValue);
            case "EXEC_PARAM":
                String value = context.get(paramValue.getConfig());
                return convertListByType(value, ParamTypeEnum.valueOf(param.getType().toUpperCase(Locale.ROOT)));
            default:
                throw new IllegalArgumentException("not support param from:" + paramValue.getFrom());
        }
    }

    public static Object singleValue(Map<String, String> context, Param param, ParamValue paramValue) throws Exception {
        switch (paramValue.getFrom().toUpperCase(Locale.ROOT)) {
            case "SQL_QUERY":
                return singleJdbcValue(context, param, paramValue);
            case "FUNCTION":
                return functionValue(context, param, paramValue).get(0);
            case "CONSTANT":
                return singleConstantValue(context, param, paramValue);
            case "EXEC_PARAM":
                String value = context.get(paramValue.getConfig());
                return convertByType(value, ParamTypeEnum.valueOf(param.getType().toUpperCase(Locale.ROOT)));
            default:
                throw new IllegalArgumentException("not support param from:" + paramValue.getFrom());
        }
    }

    // 函数config的json对象
    @Data
    private static class FunctionConfig {
        private AnchorPointWithOffset date;
        // 是否取锚点和偏移量之间的所有日期
        private Boolean allDate;

        // 日期范围
        private AnchorPointWithOffset start;
        private AnchorPointWithOffset end;
        private List<String> types;
    }

    // 锚点和偏移量
    // 如果外层 allDate 为 true, 那么会取锚点和偏移量之间的所有日期
    // 否则只会取 偏移量的日期
    @Data
    private static class AnchorPointWithOffset {
        // 锚点日期
        private AnchorPoint date;
        // 计算偏移量, 负数向前推
        // 间隔天
        private Integer intervalDays;
        private Integer intervalWeeks;
        private Integer intervalMonths;
        private Integer intervalYears;
    }

    @Data
    private static class AnchorPoint {
        // 计算锚点的入参日期
        private ParamValue date;
        // 计算锚点的日期函数
        private String type;
    }

    public static List<Object> functionValue(Map<String, String> context, Param param, ParamValue paramValue) throws Exception {
        String config = paramValue.getConfig();
        Map<String, Object> childValue = paramValues(context, paramValue.getChildParams());
        FunctionConfig functionConfig = JSONUtils.parseObject(replaceSqlHolder(config, childValue), FunctionConfig.class);
        if (functionConfig.getDate() != null) {
            AnchorPointWithOffset date = functionConfig.getDate();
            LocalDate point = calAnchorPoint(context, date.date);
            LocalDate offset = calAnchorPointOffset(point, date);
            if (functionConfig.allDate != null && functionConfig.allDate) {
                return convertListObject(allDate(point, offset));
            } else {
                return Collections.singletonList(date_formatter.format(offset));
            }
        } else {
            AnchorPointWithOffset start = functionConfig.getStart();
            AnchorPointWithOffset end = functionConfig.getEnd();
            LocalDate startDate = calAnchorPointOffset(calAnchorPoint(context, start.date), start);
            LocalDate endDate = calAnchorPointOffset(calAnchorPoint(context, end.date), end);
            List<Object> result = new ArrayList<>();
            for (String type : functionConfig.getTypes()) {
                switch (type.toUpperCase(Locale.ROOT)) {
                    case "ALL_MONTH_START":
                        result.addAll(convertListObject(allMonthStart(startDate, endDate)));
                        break;
                    case "ALL_MONTH_END":
                        result.addAll(convertListObject(allMonthEnd(startDate, endDate)));
                        break;
                    default:
                        throw new IllegalArgumentException("not support range date type:" + type);
                }
            }
            return result;
        }
    }

    public static Object singleConstantValue(Map<String, String> context, Param param, ParamValue paramValue) {
        return convertByType(paramValue.getConfig(), ParamTypeEnum.valueOf(param.getType().toUpperCase(Locale.ROOT)));
    }

    public static List<Object> listConstantValue(Map<String, String> context, Param param, ParamValue paramValue) {
        return convertListByType(paramValue.getConfig(), ParamTypeEnum.valueOf(param.getType().toUpperCase(Locale.ROOT)));
    }

    public static List<Object> convertListByType(String value, ParamTypeEnum type) {
        switch (type) {
            case INTEGER:
                return convertListObject(JSONUtils.toList(value, Integer.class));
            case REAL:
                return convertListObject(JSONUtils.toList(value, Double.class));
            case DATE:
            case DATETIME:
            case STRING:
            default:
                return convertListObject(JSONUtils.toList(value, String.class));
        }
    }

    public static Object convertByType(String value, ParamTypeEnum type) {
        switch (type) {
            case INTEGER:
                return Integer.valueOf(value);
            case REAL:
                return Double.valueOf(value);
            case DATE:
            case DATETIME:
            case STRING:
            default:
                return value;
        }
    }

    public static Object singleJdbcValue(Map<String, String> context, Param param, ParamValue paramValue) throws Exception {
        Map<String, Object> childValue = paramValues(context, paramValue.getChildParams());
        String sql = paramValue.getConfig();
        sql = replaceSqlHolder(sql, childValue);
        ResultGetFunction getter = convertToDataType(ParamTypeEnum.valueOf(param.getType().toUpperCase(Locale.ROOT)));
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

//        return mock(ParamTypeEnum.valueOf(param.getType().toUpperCase(Locale.ROOT)));
    }

    public static List<Object> listJdbcValue(Map<String, String> context, Param param, ParamValue paramValue) throws Exception {
        Map<String, Object> childValue = paramValues(context, paramValue.getChildParams());
        String sql = paramValue.getConfig();
        sql = replaceSqlHolder(sql, childValue);
        ResultGetFunction getter = convertToDataType(ParamTypeEnum.valueOf(param.getType().toUpperCase(Locale.ROOT)));
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


//        return Collections.singletonList(mock(ParamTypeEnum.valueOf(param.getType().toUpperCase(Locale.ROOT))));
    }


    public static <T> List<Object> convertListObject(List<T> list) {
        List<Object> list1 = new ArrayList<>(list.size());
        list1.addAll(list);
        return list1;
    }


    public static String jdbcUrl(ReadConfig readConfig) {
        return "jdbc:" + readConfig.getType().toLowerCase(Locale.ROOT) + "//" + readConfig.getIp() + ":" + readConfig.getPort() + "/"
            + readConfig.getSchema();
    }

    private static ResultGetFunction convertToDataType(ParamTypeEnum type) {
        switch (type) {
            case INTEGER:
                return resultSet -> resultSet.getInt(1);
            case REAL:
                return resultSet -> resultSet.getDouble(1);
            case DATE:
            case DATETIME:
            case STRING:
            default:
                return resultSet -> resultSet.getString(1);
        }
    }

    @FunctionalInterface
    private interface ResultGetFunction {
        Object getFirst(ResultSet resultSet) throws SQLException;
    }

    private static Object mock(ParamTypeEnum type) {
        switch (type) {
            case INTEGER:
                return 1000;
            case REAL:
                return 1001.1;
            case DATE:
                return "2022-01-05";
            case DATETIME:
                return "2022-01-04 13:12:00";
            case STRING:
            default:
                return "abc";
        }
    }

    public static boolean numerical(String type) {
        return numericalTypes.contains(type.toUpperCase(Locale.ROOT));
    }

    /**
     * str -> select id from table_1 where name='${a}' and id = '${b}' and city in ('${c}')
     * holderValues -> a:"tom" b:4 c:["杭州","北京","上海"]
     * return -> select id from table_1 where name="to\"m" and id = 1 and city in ("杭州","北京")
     * <p>
     * ------ code -----
     * <p>
     * Map<String, Object> holderValues = new HashMap<>();
     * holderValues.put("a", "to\"m");
     * holderValues.put("b", 1);
     * List<String> list = new ArrayList<>();
     * list.add("杭州");
     * list.add("北京");
     * holderValues.put("c", list);
     * System.out.println(replaceSqlHolder("select id from table_1 where name='${a}' and id = '${b}' and city in ('${c}')", holderValues));
     * <p>
     * <p>
     * <p>
     * <p>
     * <p>
     * 字符和日期类型,替换时会添加上引号
     * 列表类型会逗号隔开: 1,2,3 / "杭州","北京","上海"
     *
     * @param sql
     * @param holderValues
     * @return
     */
    public static String replaceSqlHolder(String sql, Map<String, Object> holderValues) {
        if (sql == null || holderValues == null || holderValues.size() == 0) {
            return sql;
        }
        for (Map.Entry<String, Object> entry : holderValues.entrySet()) {
            if (entry.getValue() instanceof Collections) {
                String replaceValue = JSONUtils.toJsonString(entry.getValue());
                sql = sql.replace("'${" + entry.getKey() + "}'", replaceValue.substring(1, replaceValue.length() - 1));
            } else {
                sql = sql.replace("'${" + entry.getKey() + "}'", JSONUtils.toJsonString(entry.getValue()));
            }
        }
        return sql;
    }

    public static List<String> allMonthEnd(LocalDate startDate, LocalDate end) {
        LocalDate start = startDate.with(TemporalAdjusters.lastDayOfMonth());
        List<String> allMonthEnd = new ArrayList<>();
        while (!end.isBefore(start)) {
            allMonthEnd.add(date_formatter.format(start));
            start = start.plusMonths(1).with(TemporalAdjusters.lastDayOfMonth());
        }
        return allMonthEnd;
    }

    public static List<String> allMonthStart(LocalDate start, LocalDate endDate) {
        LocalDate end = endDate.with(TemporalAdjusters.firstDayOfMonth());
        List<String> allMonthStart = new ArrayList<>();
        while (!end.isBefore(start)) {
            allMonthStart.add(date_formatter.format(end));
            end = end.minusMonths(1).with(TemporalAdjusters.firstDayOfMonth());
        }
        return allMonthStart;
    }

    public static List<String> allDate(LocalDate start, LocalDate end) {
        if (start.isAfter(end)) {
            LocalDate tmp = start;
            start = end;
            end = tmp;
        }
        List<String> allDate = new ArrayList<>();
        while (!end.isBefore(start)) {
            allDate.add(date_formatter.format(start));
            start = start.plusDays(1);
        }
        return allDate;
    }

    public static LocalDate calAnchorPointOffset(LocalDate point, AnchorPointWithOffset anchorPointWithOffset) {
        return intervalDate(point, anchorPointWithOffset.getIntervalDays(), anchorPointWithOffset.getIntervalMonths(), anchorPointWithOffset.getIntervalYears(), anchorPointWithOffset.getIntervalWeeks());
    }

    public static LocalDate calAnchorPoint(Map<String, String> context, AnchorPoint anchorPoint) throws Exception {
        ParamValue paramValue = anchorPoint.getDate();
        String date = (String) singleValue(context, buildSingleDateParam(""), paramValue);
        LocalDate localDate = LocalDate.parse(date, date_formatter);
        switch (anchorPoint.getType().toUpperCase(Locale.ROOT)) {
            case "MONTH_END":
                return localDate.with(TemporalAdjusters.lastDayOfMonth());
            case "MONTH_START":
                return localDate.with(TemporalAdjusters.firstDayOfMonth());
            case "YEAR_START":
                return localDate.with(TemporalAdjusters.firstDayOfYear());
            case "YEAR_END":
                return localDate.with(TemporalAdjusters.lastDayOfYear());
            case "LAST_MONTH_START":
                return localDate.minusMonths(1).with(TemporalAdjusters.firstDayOfMonth());
            case "LAST_MONTH_END":
                return localDate.minusMonths(1).with(TemporalAdjusters.lastDayOfMonth());
            case "LAST_YEAR_END":
                return localDate.minusYears(1).with(TemporalAdjusters.lastDayOfYear());
            case "LAST_YEAR_START":
                return localDate.minusYears(1).with(TemporalAdjusters.firstDayOfYear());
            case "TODAY":
                return localDate;
            case "YESTERDAY":
                return localDate.minusDays(1);
            case "THE_DAY_BEFORE_YESTERDAY":
                return localDate.minusDays(2);
            default:
                throw new IllegalArgumentException("not support anchor point type:" + anchorPoint.getType());
        }
    }

    public static LocalDate intervalDate(LocalDate localDate, Integer intervalDays, Integer intervalMonths, Integer intervalYears, Integer intervalWeeks) {
        if (intervalDays != null) {
            localDate = localDate.plusDays(intervalDays);
        }
        if (intervalMonths != null) {
            localDate = localDate.plusMonths(intervalMonths);
        }
        if (intervalYears != null) {
            localDate = localDate.plusYears(intervalYears);
        }
        if (intervalWeeks != null) {
            localDate = localDate.plusWeeks(intervalWeeks);
        }
        return localDate;
    }

    public static Param buildSingleDateParam(String name) {
        Param param = new Param();
        param.setName(name);
        param.setType("DATE");
        param.setArray(false);
        return param;
    }


    public static void main(String[] args) throws Exception {
        ReadConfig readConfig = new ReadConfig();
        readConfig.setType("mysql");
        readConfig.setIp("localhost");
        readConfig.setPort(3306);
        readConfig.setSchema("db");
        readConfig.setUserName("root");
        readConfig.setPassword("123456");


        Param param = new Param();
        param.setName("xxx");
        param.setType("DATE");
        param.setArray(true);

        ParamValue sqlParamValue = new ParamValue();
        sqlParamValue.setFrom("SQL_QUERY");
        sqlParamValue.setConfig("select max(dt) from order_table");
        sqlParamValue.setReadConfig(readConfig);

        ParamValue constantParamValue = new ParamValue();
        constantParamValue.setFrom("CONSTANT");
        constantParamValue.setConfig("2022-01-04");

        ParamValue execPramValue = new ParamValue();
        execPramValue.setFrom("EXEC_PARAM");
        execPramValue.setConfig("abc");

        ParamValue function = new ParamValue();
        function.setFrom("FUNCTION");
        FunctionConfig functionConfig = new FunctionConfig();
        AnchorPointWithOffset end = new AnchorPointWithOffset();
        AnchorPoint anchorPoint = new AnchorPoint();
        anchorPoint.setDate(constantParamValue);
        anchorPoint.setType("YESTERDAY");
        end.setDate(anchorPoint);
        end.setIntervalDays(-3);

        AnchorPointWithOffset start = new AnchorPointWithOffset();
        start.setDate(anchorPoint);
        start.setIntervalMonths(-3);


        functionConfig.setDate(end);
        functionConfig.setAllDate(false);

//        functionConfig.setStart(start);
//        functionConfig.setEnd(end);
//        List<String> types = new ArrayList<>();
//        types.add("ALL_MONTH_START");
//        types.add("ALL_MONTH_END");
//        functionConfig.setTypes(types);
        function.setConfig(JSONUtils.toJsonString(functionConfig));

        List<ParamValue> paramValues = new ArrayList<>();
        paramValues.add(function);
        param.setParamValues(paramValues);

        Map<String, String> context = new HashMap<>();
        context.put("abc", "2022-01-02");
        Object o = paramValue(context, param);
        System.out.println(o);
        System.out.println(JSONUtils.toJsonString(param));
        System.out.println(JSONUtils.toJsonString(o));
    }
}
