// (c) Copyright 2019 Cloudera, Inc.
package com.cloudera.cdh.tests.parquet.columnindexfiltering;

import static com.cloudera.cdh.test.common.Component.HIVE;
import static java.util.Arrays.asList;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

import org.apache.avro.Conversion;
import org.apache.avro.data.TimeConversions;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;
import org.joda.time.format.DateTimeFormat;

import com.cloudera.cdh.test.common.Component;

enum ColumnHelper {
  BOOLEAN {
    @Override
    public Type getSchema() {
      return Types.optional(PrimitiveType.PrimitiveTypeName.BOOLEAN)
        .named(getColumnName());
    }

    @Override
    public String getSqlType() {
      return "BOOLEAN";
    }

    // Casting a string to boolean does not work as expected, so we can't use the usual
    // CAST('value' as type) magic for booleans.
    @Override
    public String valueToSqlLiteral(Object value) {
      if (value == null) {
        return "NULL";
      }
      return value.toString();
    }

    @Override
    public TestValues<?> getValues() {
      return new TestValues<Boolean>(false, false, true, true);
    }
  },
  INT32 {
    @Override
    public Type getSchema() {
      return Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
        .named(getColumnName());
    }

    @Override
    public String getSqlType() {
      return "INT";
    }

    @Override
    public TestValues<?> getValues() {
      return new TestValues<Integer>(Integer.MIN_VALUE, -42, 42, Integer.MAX_VALUE);
    }
  },
  INT64 {
    @Override
    public Type getSchema() {
      return Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
        .named(getColumnName());
    }

    @Override
    public String getSqlType() {
      return "BIGINT";
    }

    @Override
    public TestValues<?> getValues() {
      // Hive can't handle the two most extreme Long values.
      return new TestValues<Long>(Long.MIN_VALUE + 1, -42L, 42L, Long.MAX_VALUE - 1);
    }
  },
  INT96 {
    @Override
    public Type getSchema() {
      return Types.optional(PrimitiveType.PrimitiveTypeName.INT96)
        .named(getColumnName());
    }

    @Override
    public String getSqlType() {
      return "TIMESTAMP";
    }

    @Override
    public Object valueToFileFormatValue(Object value) {
      DateTime dateTime = (DateTime) value;
      return new NanoTime((int) DateTimeUtils.toJulianDayNumber(dateTime.getMillis()),
        dateTime.getMillisOfDay() * 1000_000L);
    }

    @Override
    public String nonNullValueToString(Object value) {
      DateTime dateTime = (DateTime) value;
      return dateTime.toString(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS"));
    }

    @Override
    public TestValues<?> getValues() {
      return new TestValues<DateTime>(
          new DateTime(1885, 1, 1, 0, 0, 0, 0),
          new DateTime(1955, 11, 5, 6, 0, 0, 0),
          new DateTime(1985, 10, 26, 1, 22, 0, 0),
          new DateTime(2015, 10, 21, 16, 29, 0, 0));
    }
  },
  FLOAT {
    @Override
    public Type getSchema() {
      return Types.optional(PrimitiveType.PrimitiveTypeName.FLOAT)
        .named(getColumnName());
    }

    @Override
    public String getSqlType() {
      return "FLOAT";
    }

    @Override
    public TestValues<?> getValues() {
      return new TestValues<Float>(-Float.MAX_VALUE, -Float.MIN_VALUE, Float.MIN_VALUE, Float.MAX_VALUE);
    }
  },
  DOUBLE {
    @Override
    public Type getSchema() {
      return Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE)
        .named(getColumnName());
    }

    @Override
    public String getSqlType() {
      return "DOUBLE";
    }

    @Override
    public TestValues<?> getValues() {
      return new TestValues<Double>(-Double.MAX_VALUE, -Double.MIN_VALUE, Double.MIN_VALUE, Double.MAX_VALUE);
    }
  },
  FIXED {
    @Override
    public Type getSchema() {
      return Types.optional(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY).length(3)
        .named(getColumnName());
    }

    @Override
    public String getSqlType() {
      return "STRING";
    }

    @Override
    public boolean isSupported(Component component) {
      return component == HIVE;
    }

    @Override
    public Object valueToFileFormatValue(Object value) {
      return Binary.fromString((String) value);
    }

    @Override
    public TestValues<?> getValues() {
      return new TestValues<String>("BMP", "GIF", "JPG", "PNG");
    }
  },
  BINARY {
    @Override
    public Type getSchema() {
      return Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
        .named(getColumnName());
    }

    @Override
    public String getSqlType() {
      return "STRING";
    }

    @Override
    public Object valueToFileFormatValue(Object value) {
      return Binary.fromString((String) value);
    }

    @Override
    public TestValues<?> getValues() {
      return new TestValues<String>("Alfa", "Bravo", "Charlie", "Delta");
    }
  },
  UTF8 {
    @Override
    public Type getSchema() {
      return Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
        .as(OriginalType.UTF8)
        .named(getColumnName());
    }

    @Override
    public String getSqlType() {
      return "STRING";
    }

    @Override
    public Object valueToFileFormatValue(Object value) {
      return Binary.fromString((String) value);
    }

    @Override
    public TestValues<?> getValues() {
      return new TestValues<String>("Alfa", "Bravo", "Charlie", "Delta");
    }
  },
  DECIMAL_IN_INT32 {
    private int precision = 9;
    private int scale = 2;

    @Override
    public Type getSchema() {
      return Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
        .as(OriginalType.DECIMAL)
        .precision(precision)
        .scale(scale)
        .named(getColumnName());
    }

    @Override
    public String getSqlType() {
      return String.format("DECIMAL(%d,%d)", precision, scale);
    }

    @Override
    public boolean isSupported(Component component) {
      return component != HIVE;
    }

    @Override
    public Object valueToFileFormatValue(Object value) {
      return ((BigDecimal) value).setScale(scale).unscaledValue().intValue();
    }

    @Override
    public TestValues<?> getValues() {
      return new TestValues<BigDecimal>(
          new BigDecimal("-1234567.89"),
          new BigDecimal("-42"),
          new BigDecimal("42"),
          new BigDecimal("1234567.89"));
    }
  },
  DECIMAL_IN_INT64 {
    private int precision = 18;
    private int scale = 4;

    @Override
    public Type getSchema() {
      return Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
        .as(OriginalType.DECIMAL)
        .precision(precision)
        .scale(scale)
        .named(getColumnName());
    }

    @Override
    public String getSqlType() {
      return String.format("DECIMAL(%d,%d)", precision, scale);
    }

    @Override
    public boolean isSupported(Component component) {
      return component != HIVE;
    }

    @Override
    public Object valueToFileFormatValue(Object value) {
      return ((BigDecimal) value).setScale(scale).unscaledValue().longValue();
    }

    @Override
    public TestValues<?> getValues() {
      return new TestValues<BigDecimal>(
          new BigDecimal("-12345678912345.6789"),
          new BigDecimal("-42"),
          new BigDecimal("42"),
          new BigDecimal("12345678912345.6789"));
    }
  },
  DECIMAL_IN_FIXED {
    private int precision = 18;
    private int scale = 6;

    private final byte[] minValue = new byte[8];
    {
      Arrays.fill(minValue, (byte) 0xFF);
    }

    @Override
    public Type getSchema() {
      return Types.optional(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
        .length(8)
        .as(OriginalType.DECIMAL)
        .precision(precision)
        .scale(scale)
        .named(getColumnName());
    }

    @Override
    public String getSqlType() {
      return String.format("DECIMAL(%d,%d)", precision, scale);
    }

    @Override
    public Object valueToFileFormatValue(Object value) {
      BigDecimal decimal = (BigDecimal) value;
      byte[] array = decimal.setScale(scale).unscaledValue().toByteArray();
      if (array.length < 8) {
        byte[] dest = new byte[8];
        System.arraycopy(array, 0, dest, dest.length - array.length, array.length);

        // As we are storing the value in two's complement we have to fill the first bytes with FF
        if (decimal.signum() < 0)
          System.arraycopy(minValue, 0, dest, 0, dest.length - array.length);

        array = dest;
      }
      return Binary.fromConstantByteArray(array);
    }

    @Override
    public TestValues<?> getValues() {
      return new TestValues<BigDecimal>(
          new BigDecimal("-123456789123.456789"),
          new BigDecimal("-42"),
          new BigDecimal("42"),
          new BigDecimal("123456789123.456789"));
    }
  },
  DECIMAL_IN_BINARY {
    private int precision = 38;
    private int scale = 12;

    @Override
    public Type getSchema() {
      return Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
        .as(OriginalType.DECIMAL)
        .precision(precision)
        .scale(scale)
        .named(getColumnName());
    }

    @Override
    public String getSqlType() {
      return String.format("DECIMAL(%d,%d)", precision, scale);
    }

    @Override
    public Object valueToFileFormatValue(Object value) {
      BigDecimal decimal = (BigDecimal) value;
      return Binary.fromConstantByteArray(decimal.setScale(scale).unscaledValue().toByteArray());
    }

    @Override
    public TestValues<?> getValues() {
      return new TestValues<BigDecimal>(
          new BigDecimal("-12345678901234567890123456.789012345678"),
          new BigDecimal("-42"),
          new BigDecimal("42"),
          new BigDecimal("12345678901234567890123456.789012345678"));
    }
  };

  static class TestValues<T> {
    public TestValues(T a, T b, T c, T d) {
      values = asList(a, b, c, d);
    }
    public List<T> values;
  }

  public String getColumnName() {
    return name().toLowerCase() + "_col";
  }

  public boolean isSupported(Component component) {
    return true;
  }

  public Object valueToFileFormatValue(Object value) {
    return value;
  }

  public String nonNullValueToString(Object value) {
    return value.toString();
  }

  public String valueToSqlLiteral(Object value) {
    if (value == null) {
      return "NULL";
    }
    String valueAsString = nonNullValueToString(value);
    if (valueAsString.contains("'")) {
      throw new IllegalArgumentException();
    }
    return String.format("CAST('%s' AS %s)", valueAsString, getSqlType());
  }

  abstract public TestValues<?> getValues();
  abstract public String getSqlType();
  abstract public Type getSchema();
}
