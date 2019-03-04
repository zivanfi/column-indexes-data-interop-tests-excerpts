// (c) Copyright 2019 Cloudera, Inc.
package com.cloudera.cdh.tests.parquet.columnindexfiltering;

import static com.cloudera.cdh.test.utils.StorageFormat.PARQUET;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.example.ExampleOutputFormat;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.apache.parquet.schema.Types.GroupBuilder;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.cdh.test.common.Editable;
import com.cloudera.cdh.test.common.SQLTextQueryable;
import com.cloudera.cdh.test.utils.Compression;
import com.cloudera.cdh.test.utils.HDFSClient;
import com.cloudera.cdh.test.utils.HiveClient;
import com.cloudera.cdh.test.utils.ImpalaClient;
import com.cloudera.cdh.test.utils.PresetUtils;
import com.cloudera.cdh.test.utils.SparkSQLClient;
import com.cloudera.cdh.test.utils.StorageFormat;
import com.cloudera.cdh.test.utils.TableReference;
import com.cloudera.cdh.test.utils.TestIterationParametersBuilder;
import com.cloudera.cdh.tests.parquet.columnindexfiltering.ColumnHelper.TestValues;
import com.cloudera.itest.parameters.ParamAnnotation;
import com.cloudera.itest.parameters.ParamVector;
import com.cloudera.itest.testsuite.DataInterop;
import com.google.common.collect.ImmutableMap;

@Category(DataInterop.class)
@RunWith(Parameterized.class)
public class ColumnIndexFiltering {
  private static final Logger LOGGER = LoggerFactory.getLogger(ColumnIndexFiltering.class);

  // We want to reuse tables between individual test cases.
  private static Map<Editable, TableReference> tblRefMap = new HashMap<>();
  private static final FileSystem fs = getFileSystem();

  private final List<ColumnHelper> supportedHelpers;
  private final SQLTextQueryable reader;
  private final Editable writer;
  private final ColumnHelper columnHelper;
  private final String operator;
  private final StorageFormat format;
  private Path workingDir;
  private TableReference tblRef;
  private Compression compression;

  public ColumnIndexFiltering(ParamVector.Builder<TestIterationParametersBuilder.TestIterationParameters> builder)
    throws Exception {
    TestIterationParametersBuilder.TestIterationParameters params = builder.build();
    reader = (SQLTextQueryable) params.getReader();
    writer = params.getWriter();
    columnHelper = (ColumnHelper) params.getCustomData1();
    operator = (String) params.getCustomData2();
    format = params.getStorageFormat();
    compression = params.getCompression();
    params.checkAllParamsRetrieved();

    List<ColumnHelper> columnList = new ArrayList<>();
    for (ColumnHelper helper : ColumnHelper.values())
      if (helper.isSupported(reader.getComponent()))
        columnList.add(helper);
    supportedHelpers = Collections.unmodifiableList(columnList);
    tblRef = tblRefMap.get(params.getWriter());
    if (tblRef == null) {
      String prefix = getClass().getSimpleName() + '_' + writer.getComponent().name();
      tblRef = TableReference.generateTableReference(prefix);
      workingDir = getWorkingDir(tblRef);
      createDir(workingDir);
      createParquetTable();
      tblRefMap.put(params.getWriter(), tblRef);
    }
  }

  @AfterClass
  public static void cleanup() throws Exception {
    if (TableReference.KEEP_TABLES) {
      return;
    }
    for (Map.Entry<Editable, TableReference> entry : tblRefMap.entrySet()) {
      TableReference tblRef = entry.getValue();
      Editable component = entry.getKey();
      fs.delete(getWorkingDir(tblRef), true);
      tblRef.drop(component);
    }
  }

  @Parameterized.Parameters(name = "{0}")
  public static List<ParamVector.Builder<TestIterationParametersBuilder.TestIterationParameters>[]> getTestDimensions()
    throws Exception {
    List<ColumnHelper> columnHelpers = asList(ColumnHelper.values());
    List<String> operators = PresetUtils.getByPropertyList(
        asList("columnindexfiltering.operators"),
        ImmutableMap.of(
            "default", asList("="),
            "all", asList("=", "<", ">", "<=", ">=", "!=", "IS", "IS NOT")
        ));
    return new TestIterationParametersBuilder()
      .setWriters(new HiveClient(), new ImpalaClient(), new SparkSQLClient())
      .setReaders(new HiveClient(), new ImpalaClient(), new SparkSQLClient())
      .addSameReaderWriterRule(ParamAnnotation.NOT_TO_BE_TESTED)
      .setCustomData1(columnHelpers)
      .setCustomData2(operators)
      .setFormats(PARQUET)
      .setCompressions(Compression.UNCOMPRESSED)
      .build();
  }

  private static MessageType createParquetSchema(List<ColumnHelper> columns) {
    GroupBuilder<MessageType> builder = Types.buildMessage();
    builder = builder.addField(Types.optional(PrimitiveType.PrimitiveTypeName.INT32).named("id"));
    for (ColumnHelper helper : columns) {
      builder = builder.addField(helper.getSchema());
    }
    return builder.named("record");
  }

  private void createParquetTable() throws Exception {
    writeParquetFiles();

    StringBuilder builder = new StringBuilder("CREATE EXTERNAL TABLE ").append(tblRef).append("(id int, ");
    String sep = "";
    for (ColumnHelper helper : supportedHelpers) {
      builder.append(sep).append(helper.getColumnName()).append(' ').append(helper.getSqlType());
      sep = ", ";
    }
    builder.append(")\nSTORED AS PARQUET\nLOCATION '").append(workingDir).append('\'');
    writer.executeSqlUpdate(builder.toString());
  }

  /**
   * Generate values according to the test plan
   */
  private List<List<List<Object>>> generateColumnOrientedData() {
    // Add IDs as the first column
    List<List<List<Object>>> valuesByFileAndCol = asList(
        new ArrayList<>(asList(asList(
            1, 2, 3,
            4, 5, 6,
            7))),
        new ArrayList<>(asList(asList(
            8, 9, 10,
            11, 12, 13,
            14, 15, 16))),
        new ArrayList<>(asList(asList(
            17, 18, 19,
            20, 21, 22,
            23))));
    for (ColumnHelper col : supportedHelpers) {
      TestValues<?> v = col.getValues();
      Object a = v.values.get(0);
      Object b = v.values.get(1);
      Object c = v.values.get(2);
      Object d = v.values.get(3);
      Object N = null;
      valuesByFileAndCol.get(0).add(asList(
          a, b, c,
          b, c, d,
          N));
      valuesByFileAndCol.get(1).add(asList(
          N, c, d,
          N, N, N,
          a, c, N));
      valuesByFileAndCol.get(2).add(asList(
          b, N, d,
          a, N, a,
          b));
    }
    return valuesByFileAndCol;
  }

  /**
   * Transposes the inner matrix in the generated data for usage in the row-oriented API.
   */
  private List<List<List<Object>>> convertColumnOrientedDataToRowOriented(
      List<List<List<Object>>> valuesByFileAndCol) {
    List<List<List<Object>>> valuesByFileAndRow =
        asList(new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
    for (int fileNo = 0; fileNo < valuesByFileAndCol.size(); ++fileNo) {
      List<List<Object>> colsInFile = valuesByFileAndCol.get(fileNo);
      List<List<Object>> rowsInFile = valuesByFileAndRow.get(fileNo);
      int rowCount = colsInFile.get(0).size();
      for (int i = 0; i < rowCount; ++i) {
        List<Object> valuesInRow = new ArrayList<>();
        for (List<Object> col : colsInFile) {
          valuesInRow.add(col.get(i));
        }
        rowsInFile.add(valuesInRow);
      }
    }
    return valuesByFileAndRow;
  }

  private void writeDataToParquetFiles(List<List<List<Object>>> valuesByFileAndRow) throws Exception {
    MessageType schema = createParquetSchema(supportedHelpers);
    fs.mkdirs(workingDir, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
    Configuration conf = new Configuration();
    conf.setInt("parquet.page.row.count.limit", 3);
    for (int fileNo = 0; fileNo < valuesByFileAndRow.size(); ++fileNo) {
      List<List<Object>> rowsInFile = valuesByFileAndRow.get(fileNo);
      Path parquetFile = new Path(workingDir, String.format("%d.parquet", fileNo));
      GroupWriteSupport.setSchema(schema, conf);
      ExampleOutputFormat outputFormat = new ExampleOutputFormat();
      RecordWriter<Void, Group> writer =
          outputFormat.getRecordWriter(conf, parquetFile, compression.getCompressionCodecName());
      for (List<Object> row : rowsInFile)
        writer.write(null, createParquetGroup(schema, row));
      writer.close(null);
    }
  }

  private void writeParquetFiles() throws Exception {
    List<List<List<Object>>> valuesByFileAndCol = generateColumnOrientedData();
    List<List<List<Object>>> valuesByFileAndRow =
        convertColumnOrientedDataToRowOriented(valuesByFileAndCol);
    writeDataToParquetFiles(valuesByFileAndRow);
  }

  private Group createParquetGroup(MessageType schema, List<Object> row) {
    Group group = new SimpleGroup(schema.asGroupType());
    // First value is the ID
    group.add(0, (Integer) row.get(0));
    for (int i = 1, n = row.size(); i < n; ++i) {
      if (row.get(i) != null) {
        addValueToGroup(group, i, supportedHelpers.get(i - 1).valueToFileFormatValue(row.get(i)));
      }
    }
    return group;
  }

  private static void addValueToGroup(Group group, int i, Object value) {
    Class<?> c = value.getClass();
    if (c.equals(Integer.class))
      group.add(i, (Integer) value);
    else if (c.equals(Long.class))
      group.add(i, (Long) value);
    else if (c.equals(Float.class))
      group.add(i, (Float) value);
    else if (c.equals(Double.class))
      group.add(i, (Double) value);
    else if (c.equals(String.class))
      group.add(i, (String) value);
    else if (c.equals(Boolean.class))
      group.add(i, (Boolean) value);
    else if (value instanceof Group)
      group.add(i, (Group) value);
    else if (value instanceof Binary)
      group.add(i, (Binary) value);
    else if (value instanceof NanoTime)
      group.add(i, (NanoTime) value);
    else
      throw new IllegalArgumentException("Unsupported type for parquet write: " + c);
  }

  private void createDir(Path dir) throws IOException {
    fs.mkdirs(dir, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
    LOGGER.info("Directory \"{}\" created.", dir);
  }

  private static Path getWorkingDir(TableReference tblRef) throws Exception {
    return new Path(fs.getHomeDirectory(), tblRef.getName());
  }

  private void validateQueryResults(String query) throws Exception {
    reader.setPredicatePushdown(false); // Disable PPD
    List<List<Object>> referenceRows = reader.executeTextQueryReturningObjects(query);
    reader.setPredicatePushdown(true); // Enable PPD
    List<List<Object>> actualRows = reader.executeTextQueryReturningObjects(query);
    validateData(referenceRows, actualRows);
  }

  @Test
  public void testFiltering() throws Exception {
    String query = "SELECT id, %s FROM %s WHERE %s %s %s ORDER BY id";
    assumeTrue("Skipping unsupported column type.", supportedHelpers.contains(columnHelper));
    String columnName = columnHelper.getColumnName();
    switch (operator) {
    case "IS":
    case "IS NOT":
      validateQueryResults(String.format(query, columnName, tblRef, columnName, operator, "NULL"));
      break;
    default:
      for (Object refValue : columnHelper.getValues().values) {
        validateQueryResults(String.format(query, columnName, tblRef, columnName, operator,
            columnHelper.valueToSqlLiteral(refValue)));
      }
      break;
    }
  }

  private String rowsToString(List<List<Object>> rows) {
    StringJoiner table = new StringJoiner("\n");
    for (int i = 0, n = rows.size(); i < n; ++i) {
      List<Object> row = rows.get(i);
      StringJoiner line = new StringJoiner("\t");
      for (int j = 0, m = row.size(); j < m; ++j) {
        Object value = row.get(j);
        line.add(value == null ? "NULL" : value.toString());
      }
      table.add(line.toString());
    }
    return table.toString();
  }

  private void validateData(List<List<Object>> referenceRows, List<List<Object>> actualRows) {
    // Using strings makes evaluation of test failures much easier, because the whole result set can be seen at the same time
    String reference = rowsToString(referenceRows);
    String actual = rowsToString(actualRows);
    assertEquals(reference, actual);
  }

  private static FileSystem getFileSystem() {
    try {
      return new HDFSClient().getFileSystem();
    } catch (Exception e) {
      LOGGER.error("Could not get filesystem.", e);
      return null;
    }
  }
}
