package org.apache.iotdb.db.engine.profile;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.math3.stat.descriptive.rank.Median;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.*;

public class ProfileComputationTask2 implements Runnable {
  private long time1 = 0;
  private long time2 = 0;
  private long time3 = 0;
  private long time4 = 0;

  private static final RDBMS DB = RDBMS.getInstance();

  private static final Logger LOGGER = LoggerFactory.getLogger(ProfileComputationTask2.class);
  private static final String DATA_DIR =
      "D:\\Work\\Projects\\iotdb\\data\\data\\sequence\\root.test\\0\\0";

  public void scanOneTsFile(
      String tsFilePath, Collection<Modification> allModifications) throws IOException {
    long start = System.currentTimeMillis();

    TsFileSequenceReader reader = new TsFileSequenceReader(tsFilePath);
    List<Path> seriesPaths = reader.getAllPaths();
    for (Path seriesPath : seriesPaths) {
      FileSeriesStat fileSeriesStat = new FileSeriesStat(seriesPath, tsFilePath);

      List<Modification> modifications = new ArrayList<>();
      for (Modification modification : allModifications) {
        if (modification.getPath().matchFullPath((PartialPath) seriesPath)) {
          modifications.add(modification);
        }
      }
      List<IChunkReader> chunkReaders = UtilFunc.getChunkReaders(seriesPath, reader, modifications);

      for (IChunkReader chunkReader : chunkReaders) {
        fileSeriesStat.startNewChunk();
        List<IPageReader> pageReaders = chunkReader.loadPageReaderList();
        try {
          for (IPageReader pageReader : pageReaders) {
            BatchData batchData = pageReader.getAllSatisfiedPageData();

            SeriesStat stat = new SeriesStat(batchData);
            fileSeriesStat.addPageSeriesStat(stat);
          }
          fileSeriesStat.endChunk();
        } catch (IOException | ArrayIndexOutOfBoundsException error) {
          error.printStackTrace();
          LOGGER.error(error.getMessage());
        }
      }

      fileSeriesStat.endFile();
    }
    long end = System.currentTimeMillis();
//    LOGGER.info(String.format("Calc time: %dms", end - start));
  }

  @Override
  public void run() {
    Map<String, Long> allTsFiles = UtilFunc.getAllTsFiles();
    List<String> allTsFilePaths = DB.getAllTsFilesNeedUpdating(allTsFiles);
    if (allTsFilePaths.isEmpty()) {
      return;
    }

    List<Exception> errors = new ArrayList<>();
    long start = System.currentTimeMillis();
    for (int i = 0; i < allTsFilePaths.size(); i++) {
      String tsFilePath = allTsFilePaths.get(i);
      LOGGER.info(String.format("%d: %s", i, tsFilePath));

      Collection<Modification> allModifications =
          new TsFileResource(new File(tsFilePath)).getModFile().getModifications();
      try {
        scanOneTsFile(tsFilePath, allModifications);
      } catch (Exception ignored) {
      }
    }

    long end = System.currentTimeMillis();
    LOGGER.info(String.format("Time elapsed: %ds.", (end - start) / 1000));
    LOGGER.info(String.format("统计类：%ds\t空值类：%ds\t乱序类：%ds\t异常类：%ds",
        time1 / 1000,
        time4 / 1000,
        time2 / 1000,
        time3 / 1000));

    for (Exception e : errors) {
      e.printStackTrace();
      System.out.println(e.getMessage());
    }
  }

  static class UtilFunc {

    public static double[] speed(double[] origin, double[] time) {
      int n = origin.length;
      double[] speed = new double[n - 1];
      for (int i = 0; i < n - 1; i++) {
        speed[i] = (origin[i + 1] - origin[i]) / (time[i + 1] - time[i]);
      }
      return speed;
    }

    public static double[] speed(double[] origin, long[] time) {
      int n = origin.length;
      double[] speed = new double[n - 1];
      for (int i = 0; i < n - 1; i++) {
        speed[i] = (origin[i + 1] - origin[i]) / (time[i + 1] - time[i]);
      }
      return speed;
    }

    public static int[] variation(int[] origin) {
      int n = origin.length;
      int[] var = new int[n - 1];
      for (int i = 0; i < n - 1; i++) {
        var[i] = origin[i + 1] - origin[i];
      }
      return var;
    }

    public static double[] variation(long[] origin) {
      int n = origin.length;
      double[] var = new double[n - 1];
      for (int i = 0; i < n - 1; i++) {
        var[i] = (double) (origin[i + 1] - origin[i]);
      }
      return var;
    }

    public static double[] variation(double[] origin) {
      int n = origin.length;
      double[] var = new double[n - 1];
      for (int i = 0; i < n - 1; i++) {
        var[i] = origin[i + 1] - origin[i];
      }
      return var;
    }

    public static double mad(double[] value) {
      Median median = new Median();
      double mid = median.evaluate(value);
      double[] d = new double[value.length];
      for (int i = 0; i < value.length; i++) {
        d[i] = Math.abs(value[i] - mid);
      }
      return 1.4826 * median.evaluate(d);
    }

    public static double[] toDoubleArray(ArrayList<Double> list) {
      return list.stream().mapToDouble(Double::valueOf).toArray();
    }

    public static Map<String, Long> getAllTsFiles() {
      Map<String, Long> allTsFiles = new HashMap<>();
      File tmpFile = new File(DATA_DIR);
      File[] files = tmpFile.listFiles();
      if (files == null || files.length == 0) {
        return allTsFiles;
      }

      for (File file : files) {
        if (!file.isDirectory() && file.getName().endsWith("tsfile")) {
          String filePath = file.getAbsolutePath();
          Long fileVersion = UtilFunc.getFileVersion(filePath);
          allTsFiles.put(filePath, fileVersion);
        }
      }
      return allTsFiles;
    }

    public static List<IChunkReader> getChunkReaders(
        Path tsPath, TsFileSequenceReader reader, List<Modification> modifications)
        throws IOException {
      List<ChunkMetadata> chunkMetadataList = reader.getChunkMetadataList(tsPath, true);
      if (!modifications.isEmpty()) {
        QueryUtils.modifyChunkMetaData(chunkMetadataList, modifications);
      }

      List<IChunkReader> chunkReaders = new LinkedList<>();
      for (ChunkMetadata metadata : chunkMetadataList) {
        TSDataType dataType = metadata.getDataType();
        if (dataType != TSDataType.DOUBLE
            && dataType != TSDataType.FLOAT
            && dataType != TSDataType.INT32
            && dataType != TSDataType.INT64) {
          continue;
        }

        Chunk chunk = reader.readMemChunk(metadata);
        IChunkReader chunkReader = new ChunkReader(chunk, null);
        chunkReaders.add(chunkReader);
      }
      return chunkReaders;
    }


    public static long getFileVersion(String tsFilePath) {
      TsFileResource tsFileResource = new TsFileResource(new File(tsFilePath));
      long tsFileSize = tsFileResource.getTsFileSize();
      long modFileSize = new File(tsFileResource.getModFile().getFilePath()).length();
      return tsFileSize + modFileSize;
    }
  }

  class FileSeriesStat {
    private int fid = -1;
    private int sid = -1;
    private Path tsPath;
    private String filePath;
    private int chunkIndex = -1;
    private SeriesStat fileStat;
    private SeriesStat chunkStat;
    private List<SeriesStat> pageStats;

    FileSeriesStat(Path tsPath, String filePath) {
      this.tsPath = tsPath;
      this.filePath = filePath;
      fileStat = new SeriesStat();
    }

    public void addPageSeriesStat(SeriesStat stat) {
      pageStats.add(stat);
      chunkStat.merge(stat);
    }

    public void startNewChunk() {
      chunkIndex++;
      chunkStat = new SeriesStat();
      pageStats = new ArrayList<>();
    }

    public void endChunk() {
      fileStat.merge(chunkStat);
      if (chunkIndex == 0) {
        // the first chunk in this TsFile
        sid = DB.writeToSeries(tsPath);
        fid = DB.writeToFile(filePath, UtilFunc.getFileVersion(filePath));
      }
      DB.writeOneChunkStat(fid, sid, this);
    }

    public void endFile() {
      DB.writeToFileSeriesStat(fid, sid, fileStat);
    }
  }

  class SeriesStat {
    private int cnt;
    private int missCnt = 0;
    private int specialCnt = 0;
    private int lateCnt = 0;
    private int redundancyCnt = 0;
    private int valueCnt = 0;
    private int variationCnt = 0;
    private int speedCnt = 0;
    private int speedChangeCnt = 0;
    private long startTimestamp = Long.MAX_VALUE;
    private long endTimestamp = Long.MIN_VALUE;
    private double[] valueList;
    private double[] timeList;

    SeriesStat() {}

    SeriesStat(BatchData batchData) throws IOException {
      if (batchData.isEmpty()) {
        return;
      }
      long startTime = System.currentTimeMillis();
      cnt = batchData.length();
      startTimestamp = batchData.getMinTimestamp();
      endTimestamp = batchData.getMaxTimestamp();
      long endTime = System.currentTimeMillis();
      time1 += endTime - startTime;

      startTime = System.currentTimeMillis();
      ArrayList<Double> time = new ArrayList<>();
      ArrayList<Double> value = new ArrayList<>();
      while (batchData.hasCurrent()) {
        double val;
        double curTime = (double) batchData.currentTime();

        switch (batchData.getDataType()) {
          case INT32:
            val = batchData.getInt();
            break;
          case INT64:
            val = batchData.getLong();
            break;
          case FLOAT:
            val = batchData.getFloat();
            break;
          case DOUBLE:
            val = batchData.getDouble();
            break;
          default:
            throw new IOException("Unsupported data type");
        }
        if (Double.isFinite(val)) {
          time.add(curTime);
          value.add(val);
        } else {
          ++specialCnt;
          time.add(curTime);
          value.add(Double.NaN);
        }
        batchData.next();
      }
      endTime = System.currentTimeMillis();
      time4 += endTime - startTime;

      valueList = UtilFunc.toDoubleArray(value);
      timeList = UtilFunc.toDoubleArray(time);

      startTime = System.currentTimeMillis();
      timeDetect();
      endTime = System.currentTimeMillis();
      time2 += endTime - startTime;

      startTime = System.currentTimeMillis();
      valueDetect();
      endTime = System.currentTimeMillis();
      time3 += endTime - startTime;
    }

    private void valueDetect() {
      if (valueList.length < 2) {
        return;
      }
      double k = 3;
      valueCnt = findOutliers(valueList, k);
      double[] variation = UtilFunc.variation(valueList);
      variationCnt = findOutliers(variation, k);
      double[] speed = UtilFunc.speed(valueList, timeList);
      speedCnt = findOutliers(speed, k);
      if (speed.length < 2) {
        return;
      }
      double[] speedChange = UtilFunc.variation(speed);
      speedChangeCnt = findOutliers(speedChange, k);
    }

    private int findOutliers(double[] value, double k) {
      Median median = new Median();
      double mid = median.evaluate(value);
      double sigma = UtilFunc.mad(value);
      int num = 0;
      double[] var10 = value;
      int var11 = value.length;

      for (int var12 = 0; var12 < var11; ++var12) {
        double v = var10[var12];
        if (Math.abs(v - mid) > k * sigma) {
          ++num;
        }
      }

      return num;
    }

    public void timeDetect() {
      double[] interval = UtilFunc.variation(timeList);
      Median median = new Median();
      double base = median.evaluate(interval);
      ArrayList<Double> window = new ArrayList<>();

      int i;
      for (i = 0; i < Math.min(timeList.length, 10); ++i) {
        window.add(timeList[i]);
      }

      while (window.size() > 1) {
        double times = (window.get(1) - window.get(0)) / base;
        if (times <= 0.5D) {
          window.remove(1);
          ++redundancyCnt;
        } else if (times >= 2.0D && times <= 9.0D) {
          int temp = 0;

          for (int j = 2; j < window.size(); ++j) {
            double times2 = (window.get(j) - window.get(j - 1)) / base;
            if (times2 >= 2.0D) {
              break;
            }

            if (times2 <= 0.5D) {
              ++temp;
              window.remove(j);
              --j;
              if (temp == (int) Math.round(times - 1.0D)) {
                break;
              }
            }
          }

          lateCnt += temp;
          missCnt = (int) ((long) missCnt + (Math.round(times - 1.0D) - (long) temp));
        }

        window.remove(0);

        while (window.size() < 10 && i < timeList.length) {
          window.add(timeList[i]);
          ++i;
        }
      }
    }

    public void merge(SeriesStat seriesStat) {
      cnt += seriesStat.cnt;
      missCnt += seriesStat.missCnt;
      specialCnt += seriesStat.specialCnt;
      lateCnt += seriesStat.lateCnt;
      redundancyCnt += seriesStat.redundancyCnt;
      valueCnt += seriesStat.valueCnt;
      variationCnt += seriesStat.variationCnt;
      speedCnt += seriesStat.speedCnt;
      speedChangeCnt += seriesStat.speedChangeCnt;
      startTimestamp = Math.min(startTimestamp, seriesStat.startTimestamp);
      endTimestamp = Math.max(endTimestamp, seriesStat.endTimestamp);
    }
  }

  static class SQLConstant {
    public static final String JDBC_URL = "jdbc:sqlite:/d:/ships.db";
    public static final String CONN_TIMEOUT = "1000";
    public static final String MAX_SIZE = "100";
    public static final String DB_PREFIX = "profile_";

    public static final String CLOSE_SYNC_SQL = "PRAGMA synchronous=OFF";
    public static final String FOREIGN_CONSTRAINT_SQL = "PRAGMA foreign_keys = ON";
    public static final String SERIES_TABLE_NAME = DB_PREFIX + "series";
    public static final String FILE_TABLE_NAME = DB_PREFIX + "file";
    public static final String CHUNK_TABLE_NAME = DB_PREFIX + "chunk";
    public static final String PAGE_TABLE_NAME = DB_PREFIX + "page";
    public static final String FILE_SERIES_STAT_TABLE_NAME = DB_PREFIX + "file_series_stat";
    public static final String CHUNK_SERIES_STAT_TABLE_NAME = DB_PREFIX + "chunk_series_stat";
    public static final String PAGE_SERIES_STAT_TABLE_NAME = DB_PREFIX + "page_series_stat";

    public static final String CREATE_SERIES_SQL =
        String.format(
            "CREATE TABLE IF NOT EXISTS %s(\n"
                + "    sid INTEGER PRIMARY KEY AUTOINCREMENT,\n"
                + "    ts_path VARCHAR(255) NOT NULL UNIQUE\n"
                + ");",
            SERIES_TABLE_NAME);
    public static final String CREATE_FILE_SQL =
        String.format(
            "CREATE TABLE IF NOT EXISTS %s(\n"
                + "    fid INTEGER PRIMARY KEY AUTOINCREMENT,\n"
                + "    file_version BIGINT NOT NULL,\n"
                + "    file_path VARCHAR(255) NOT NULL UNIQUE\n"
                + ");",
            FILE_TABLE_NAME);

    public static final String CREATE_CHUNK_SQL =
        String.format(
            "CREATE TABLE IF NOT EXISTS %s(\n"
                + "    cid INTEGER PRIMARY KEY AUTOINCREMENT,\n"
                + "    fid INT,\n"
                + "    sid INT,\n"
                + "    chunk_index INT,\n"
                + "    FOREIGN KEY (fid) REFERENCES %s(fid) ON DELETE CASCADE\n"
                + ");",
            CHUNK_TABLE_NAME, FILE_TABLE_NAME);

    public static final String CREATE_PAGE_SQL =
        String.format(
            "CREATE TABLE IF NOT EXISTS %s(\n"
                + "    pid INTEGER PRIMARY KEY AUTOINCREMENT,\n"
                + "    cid INT,\n"
                + "    sid INT,\n"
                + "    page_index INT,\n"
                + "    FOREIGN KEY (cid) REFERENCES %s(cid) ON DELETE CASCADE\n"
                + ");",
            PAGE_TABLE_NAME, CHUNK_TABLE_NAME);

    public static final String CREATE_FILE_SERIES_STAT_SQL =
        String.format(
            "CREATE TABLE IF NOT EXISTS %s(\n"
                + "    fid INTEGER,\n"
                + "    sid INTEGER,\n"
                + "    start_timestamp BIGINT NOT NULL,\n"
                + "    end_timestamp BIGINT NOT NULL,\n"
                + "    count INTEGER NOT NULL,\n"
                + "    miss_count INTEGER NOT NULL,\n"
                + "    special_count INTEGER NOT NULL,\n"
                + "    late_count INTEGER,\n"
                + "    redundancy_count INTEGER,\n"
                + "    value_count INTEGER,\n"
                + "    variation_count INTEGER,\n"
                + "    speed_count INTEGER,\n"
                + "    speed_change_count INTEGER,\n"
                + "    PRIMARY KEY(fid, sid),\n"
                + "    FOREIGN KEY(fid) REFERENCES %s(fid) ON DELETE CASCADE,\n"
                + "    FOREIGN KEY(sid) REFERENCES %s(sid) ON DELETE CASCADE\n"
                + ")",
            FILE_SERIES_STAT_TABLE_NAME, FILE_TABLE_NAME, SERIES_TABLE_NAME);

    public static final String CREATE_CHUNK_SERIES_STAT_SQL =
        String.format(
            "CREATE TABLE IF NOT EXISTS %s(\n"
                + "    cid INTEGER PRIMARY KEY,\n"
                + "    start_timestamp BIGINT NOT NULL,\n"
                + "    end_timestamp BIGINT NOT NULL,\n"
                + "    count INTEGER NOT NULL,\n"
                + "    miss_count INTEGER NOT NULL,\n"
                + "    special_count INTEGER NOT NULL,\n"
                + "    late_count INTEGER,\n"
                + "    redundancy_count INTEGER,\n"
                + "    value_count INTEGER,\n"
                + "    variation_count INTEGER,\n"
                + "    speed_count INTEGER,\n"
                + "    speed_change_count INTEGER,\n"
                + "    FOREIGN KEY(cid) REFERENCES %s(cid) ON DELETE CASCADE\n"
                + ")",
            CHUNK_SERIES_STAT_TABLE_NAME, CHUNK_TABLE_NAME);

    public static final String CREATE_PAGE_SERIES_STAT_SQL =
        String.format(
            "CREATE TABLE IF NOT EXISTS %s(\n"
                + "    pid INTEGER PRIMARY KEY,\n"
                + "    start_timestamp BIGINT NOT NULL,\n"
                + "    end_timestamp BIGINT NOT NULL,\n"
                + "    count INTEGER NOT NULL,\n"
                + "    miss_count INTEGER NOT NULL,\n"
                + "    special_count INTEGER NOT NULL,\n"
                + "    late_count INTEGER,\n"
                + "    redundancy_count INTEGER,\n"
                + "    value_count INTEGER,\n"
                + "    variation_count INTEGER,\n"
                + "    speed_count INTEGER,\n"
                + "    speed_change_count INTEGER,\n"
                + "    FOREIGN KEY(pid) REFERENCES %s(pid) ON DELETE CASCADE\n"
                + ")",
            PAGE_SERIES_STAT_TABLE_NAME, PAGE_TABLE_NAME);
  }

  static class RDBMS {
    private final Logger logger = LoggerFactory.getLogger(RDBMS.class);
    private final DataSource ds;
    private PreparedStatement querySeriesStmt;
    private PreparedStatement writeToSeriesStmt;
    private PreparedStatement writeToFileStmt;
    private PreparedStatement writeToChunkStmt;
    private PreparedStatement writeToPageStmt;
    private PreparedStatement writeToFileSeriesStmt;
    private PreparedStatement writeToChunkSeriesStmt;
    private PreparedStatement writeToPageSeriesStmt;

    private static class RDBMSInstance {
      private RDBMSInstance() {}

      private static final RDBMS INSTANCE = new RDBMS();
    }

    public static RDBMS getInstance() {
      return RDBMSInstance.INSTANCE;
    }

    private RDBMS() {
      ds = initDataSource();
      createTablesIfNotExist();
    }

    private DataSource initDataSource() {
      HikariConfig config = new HikariConfig();
      config.setJdbcUrl(SQLConstant.JDBC_URL);
      config.addDataSourceProperty("connectionTimeout", SQLConstant.CONN_TIMEOUT);
      config.addDataSourceProperty("maximumPoolSize", SQLConstant.MAX_SIZE);
      return new HikariDataSource(config);
    }

    private void initPreparedStatements(Connection conn) throws SQLException {
      querySeriesStmt = conn.prepareStatement(
          String.format("SELECT sid FROM %s WHERE ts_path=?",
              SQLConstant.SERIES_TABLE_NAME));
      writeToSeriesStmt = conn.prepareStatement(
          String.format(
              "INSERT OR IGNORE INTO %s VALUES (?,?)",
              SQLConstant.SERIES_TABLE_NAME),
          Statement.RETURN_GENERATED_KEYS);
      writeToFileStmt = conn.prepareStatement(
          String.format("INSERT OR IGNORE INTO %s VALUES (?,?,?)",
              SQLConstant.FILE_TABLE_NAME),
          Statement.RETURN_GENERATED_KEYS);
      writeToChunkStmt = conn.prepareStatement(
          String.format("INSERT INTO %s VALUES (?,?,?,?)",
              SQLConstant.CHUNK_TABLE_NAME),
          Statement.RETURN_GENERATED_KEYS);
      writeToPageStmt = conn.prepareStatement(
          String.format("INSERT INTO %s VALUES (?,?,?,?)",
          SQLConstant.PAGE_TABLE_NAME),
          Statement.RETURN_GENERATED_KEYS);
      writeToFileSeriesStmt = conn.prepareStatement(
          String.format("INSERT INTO %s VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
          SQLConstant.FILE_SERIES_STAT_TABLE_NAME));
      writeToChunkSeriesStmt = conn.prepareStatement(
          String.format("INSERT INTO %s VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
          SQLConstant.CHUNK_SERIES_STAT_TABLE_NAME));
      writeToPageSeriesStmt = conn.prepareStatement(
          String.format("INSERT INTO %s VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
              SQLConstant.PAGE_SERIES_STAT_TABLE_NAME));
    }

    public void createTablesIfNotExist() {
      try (Connection conn = ds.getConnection();
           Statement stmt = conn.createStatement()) {
        stmt.execute(SQLConstant.CLOSE_SYNC_SQL);
        stmt.execute(SQLConstant.FOREIGN_CONSTRAINT_SQL);
        stmt.execute(SQLConstant.CREATE_SERIES_SQL);
        stmt.execute(SQLConstant.CREATE_FILE_SQL);
        stmt.execute(SQLConstant.CREATE_CHUNK_SQL);
        stmt.execute(SQLConstant.CREATE_PAGE_SQL);
        stmt.execute(SQLConstant.CREATE_FILE_SERIES_STAT_SQL);
        stmt.execute(SQLConstant.CREATE_CHUNK_SERIES_STAT_SQL);
        stmt.execute(SQLConstant.CREATE_PAGE_SERIES_STAT_SQL);
      } catch (SQLException e) {
        logger.error("Create tables failed ", e);
      }
    }

    public int writeToSeries(Path tsPath) {
      String querySQL = String.format("SELECT sid FROM %s WHERE ts_path=?", SQLConstant.SERIES_TABLE_NAME);
      String insertSQL = String.format("INSERT INTO %s VALUES (?,?)", SQLConstant.SERIES_TABLE_NAME);
      try (Connection conn = ds.getConnection();
           PreparedStatement queryStmt = conn.prepareStatement(querySQL);
           PreparedStatement insertStmt = conn.prepareStatement(insertSQL, Statement.RETURN_GENERATED_KEYS)) {
        queryStmt.setString(1, tsPath.getFullPath());
        ResultSet rs = queryStmt.executeQuery();
        if (rs.next()) {
          return rs.getInt(1);
        }

        insertStmt.setNull(1, Types.INTEGER);
        insertStmt.setString(2, tsPath.getFullPath());
        insertStmt.executeUpdate();
        return insertStmt.getGeneratedKeys().getInt(1);
      } catch (SQLException e) {
        logger.error("Insert into series table error: ", e);
      }
      return -1;
    }

    public void writeToSeries(PreparedStatement stmt, List<Path> tsPaths) {
      try {
        for (Path tsPath : tsPaths) {
          stmt.setNull(1, Types.INTEGER);
          stmt.setString(2, tsPath.getFullPath());
          stmt.addBatch();
        }
        stmt.executeBatch();
      } catch (SQLException e) {
        logger.error("Insert into series table error: ", e);
      }
    }

    public int writeToFile(String filePath, long version) {
      String querySQL = String.format("SELECT fid FROM %s WHERE file_path=?", SQLConstant.FILE_TABLE_NAME);
      String insertSQL = String.format("INSERT OR IGNORE INTO %s VALUES (?,?,?)", SQLConstant.FILE_TABLE_NAME);
      try (Connection conn = ds.getConnection();
           PreparedStatement queryStmt = conn.prepareStatement(querySQL);
          PreparedStatement insertStmt = conn.prepareStatement(insertSQL, Statement.RETURN_GENERATED_KEYS)) {
        queryStmt.setString(1, filePath);
        ResultSet rs = queryStmt.executeQuery();
        if (rs.next()) {
          return rs.getInt(1);
        }

        insertStmt.setNull(1, Types.INTEGER);
        insertStmt.setLong(2, version);
        insertStmt.setString(3, filePath);
        insertStmt.executeUpdate();
        return insertStmt.getGeneratedKeys().getInt(1);
      } catch (SQLException e) {
        logger.error("Insert into file table error: ", e);
      }
      return -1;
    }

    public int writeToFile(PreparedStatement stmt, String filePath, long version) {
      try {
        stmt.setNull(1, Types.INTEGER);
        stmt.setLong(2, version);
        stmt.setString(3, filePath);
        stmt.executeUpdate();
        return stmt.getGeneratedKeys().getInt(1);
      } catch (SQLException e) {
        logger.error("Insert into file table error: ", e);
      }
      return -1;
    }

    public int writeToChunk(PreparedStatement stmt, int fid, int sid, int chunkIndex) {
      try {
        stmt.setNull(1, Types.INTEGER);
        stmt.setInt(2, fid);
        stmt.setInt(3, sid);
        stmt.setInt(4, chunkIndex);
        stmt.executeUpdate();
        return stmt.getGeneratedKeys().getInt(1);
      } catch (SQLException e) {
        logger.error("Insert into chunk table error: ", e);
      }
      return -1;
    }

    public int writeToPage(PreparedStatement stmt, int cid, int sid, int pageIndex) {
      try {
        stmt.setNull(1, Types.INTEGER);
        stmt.setInt(2, cid);
        stmt.setInt(3, sid);
        stmt.setInt(4, pageIndex);
        stmt.executeUpdate();
        return stmt.getGeneratedKeys().getInt(1);
      } catch (SQLException e) {
        logger.error("Insert into page table error: ", e);
      }
      return -1;
    }

    public synchronized void writeToChunkSeriesStat(
        PreparedStatement stmt, int cid, SeriesStat stat) {
      try {
        stmt.setInt(1, cid);
        stmt.setLong(2, stat.startTimestamp);
        stmt.setLong(3, stat.endTimestamp);
        stmt.setInt(4, stat.cnt);
        stmt.setInt(5, stat.missCnt);
        stmt.setInt(6, stat.specialCnt);
        stmt.setInt(7, stat.lateCnt);
        stmt.setInt(8, stat.redundancyCnt);
        stmt.setInt(9, stat.valueCnt);
        stmt.setInt(10, stat.variationCnt);
        stmt.setInt(11, stat.speedCnt);
        stmt.setInt(12, stat.speedChangeCnt);
        stmt.executeUpdate();
      } catch (SQLException e) {
        logger.error("Insert into chunk series stat table error: ", e);
      }
    }

    public synchronized void writeToPageSeriesStat(
        PreparedStatement stmt, int pid, SeriesStat stat) {
      try {
        stmt.setInt(1, pid);
        stmt.setLong(2, stat.startTimestamp);
        stmt.setLong(3, stat.endTimestamp);
        stmt.setInt(4, stat.cnt);
        stmt.setInt(5, stat.missCnt);
        stmt.setInt(6, stat.specialCnt);
        stmt.setInt(7, stat.lateCnt);
        stmt.setInt(8, stat.redundancyCnt);
        stmt.setInt(9, stat.valueCnt);
        stmt.setInt(10, stat.variationCnt);
        stmt.setInt(11, stat.speedCnt);
        stmt.setInt(12, stat.speedChangeCnt);
        stmt.executeUpdate();
      } catch (SQLException e) {
        logger.error("Insert into page series stat table error: ", e);
      }
    }

    public synchronized void writeToFileSeriesStat(int fid, int sid, SeriesStat stat) {
      String sql = String.format("INSERT INTO %s VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", SQLConstant.FILE_SERIES_STAT_TABLE_NAME);
      try (Connection conn = ds.getConnection();
          PreparedStatement stmt = conn.prepareStatement(sql)) {
        stmt.setInt(1, fid);
        stmt.setInt(2, sid);
        stmt.setLong(3, stat.startTimestamp);
        stmt.setLong(4, stat.endTimestamp);
        stmt.setInt(5, stat.cnt);
        stmt.setInt(6, stat.missCnt);
        stmt.setInt(7, stat.specialCnt);
        stmt.setInt(8, stat.lateCnt);
        stmt.setInt(9, stat.redundancyCnt);
        stmt.setInt(10, stat.valueCnt);
        stmt.setInt(11, stat.variationCnt);
        stmt.setInt(12, stat.speedCnt);
        stmt.setInt(13, stat.speedChangeCnt);
        stmt.executeUpdate();
      } catch (SQLException e) {
        logger.error("Insert into file series stat table error: ", e);
      }
    }

    public synchronized void writeToFileSeriesStat(
        PreparedStatement stmt, int fid, int sid, SeriesStat stat) {
      try {
        stmt.setInt(1, fid);
        stmt.setInt(2, sid);
        stmt.setLong(3, stat.startTimestamp);
        stmt.setLong(4, stat.endTimestamp);
        stmt.setInt(5, stat.cnt);
        stmt.setInt(6, stat.missCnt);
        stmt.setInt(7, stat.specialCnt);
        stmt.setInt(8, stat.lateCnt);
        stmt.setInt(9, stat.redundancyCnt);
        stmt.setInt(10, stat.valueCnt);
        stmt.setInt(11, stat.variationCnt);
        stmt.setInt(12, stat.speedCnt);
        stmt.setInt(13, stat.speedChangeCnt);
        stmt.executeUpdate();
      } catch (SQLException e) {
        logger.error("Insert into file series stat table error: ", e);
      }
    }

    public void writeOneChunkStat(int fid, int sid, FileSeriesStat stat) {
      try (Connection conn = ds.getConnection()) {
        conn.setAutoCommit(false);
        initPreparedStatements(conn);

        int cid = writeToChunk(writeToChunkStmt, fid, sid, stat.chunkIndex + 1);
        writeToChunkSeriesStat(writeToChunkSeriesStmt, cid, stat.chunkStat);
        for (int i = 0; i < stat.pageStats.size(); i++) {
          SeriesStat pageStat = stat.pageStats.get(i);
          int pid = writeToPage(writeToPageStmt, cid, sid, i + 1);
          writeToPageSeriesStat(writeToPageSeriesStmt, pid, pageStat);
        }

        conn.commit();
      } catch (SQLException e) {
        logger.error("Error writing one chunk statistic to sqlite: ", e);
      }
    }

    public List<String> getAllTsFilesNeedUpdating(Map<String, Long> allTsFiles) {
      String selectSQL =
          String.format(
              "SELECT file_path, file_version FROM %s",
              ProfileComputationTask.SQLConstant.FILE_TABLE_NAME);
      String deleteSQL =
          String.format(
              "DELETE FROM %s WHERE file_path=?",
              ProfileComputationTask.SQLConstant.FILE_TABLE_NAME
          );
      Map<String, Long> allRDBMSFiles = new HashMap<>();
      try (Connection conn = ds.getConnection();
           Statement stmt = conn.createStatement();
           PreparedStatement deleteStmt = conn.prepareStatement(deleteSQL)) {
        ResultSet rs = stmt.executeQuery(selectSQL);
        while (rs.next()) {
          String filePath = rs.getString(1);
          long fileVersion = rs.getLong(2);
          allRDBMSFiles.put(filePath, fileVersion);
        }

        Set<String> files = new HashSet<>(allRDBMSFiles.keySet());
        files.removeAll(allTsFiles.keySet());
        for (String filePath : files) {
          deleteStmt.setString(1, filePath);
          deleteStmt.addBatch();
        }
        deleteStmt.executeBatch();

        for (Map.Entry<String, Long> entry : allRDBMSFiles.entrySet()) {
          String filePath = entry.getKey();
          long fileVersion = entry.getValue();
          if (allTsFiles.get(filePath) != null
            && allTsFiles.get(filePath) == fileVersion) {
            allTsFiles.remove(filePath);
          }
        }
      } catch (SQLException e) {
        logger.error("Get all matching tsfile paths error: ", e);
      }

      return new ArrayList<>(allTsFiles.keySet());
    }
  }
}
