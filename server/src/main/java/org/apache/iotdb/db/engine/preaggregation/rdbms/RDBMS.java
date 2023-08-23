package org.apache.iotdb.db.engine.preaggregation.rdbms;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.preaggregation.api.FileSeriesStat;
import org.apache.iotdb.db.engine.preaggregation.api.SeriesStat;
import org.apache.iotdb.db.engine.preaggregation.api.TsFileSeriesStat;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.utils.Pair;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class RDBMS {
  private final Logger LOGGER = LoggerFactory.getLogger(RDBMS.class);
  private final DataSource ds;

  private PreparedStatement writeToChunkStmt;
  private PreparedStatement writeToPageStmt;
  private PreparedStatement writeToChunkSeriesStmt;
  private PreparedStatement writeToPageSeriesStmt;

  private final Map<AggregationType, Pair<String, String>> AGGREGATION_MAP =
      new HashMap<AggregationType, Pair<String, String>>() {
        {
          put(AggregationType.PREAGG_COUNT, new Pair<>("COUNT", SeriesStat.cntField));
          put(AggregationType.PREAGG_SUM, new Pair<>("SUM", SeriesStat.sumFiled));
          put(AggregationType.PREAGG_MIN_VALUE, new Pair<>("MIN", SeriesStat.minValueField));
          put(AggregationType.PREAGG_MAX_VALUE, new Pair<>("MAX", SeriesStat.maxValueField));
        }
      };

  private RDBMS() {
    ds = initDataSource();
    createTablesIfNotExist();
  }

  public static RDBMS getInstance() {
    return InstanceHolder.INSTANCE;
  }

  private static class InstanceHolder {
    private InstanceHolder() {}

    private static final RDBMS INSTANCE = new RDBMS();
  }

  private DataSource initDataSource() {
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(SQLConstants.JDBC_URL);
    config.addDataSourceProperty("connectionTimeout", SQLConstants.CONN_TIMEOUT);
    config.addDataSourceProperty("maximumPoolSize", SQLConstants.MAX_SIZE);
    return new HikariDataSource(config);
  }

  public void createTablesIfNotExist() {
    try (Connection conn = ds.getConnection();
        Statement stmt = conn.createStatement()) {
      stmt.execute(SQLConstants.FOREIGN_CONSTRAINT_SQL);
      stmt.execute(SQLConstants.CREATE_SERIES_SQL);
      stmt.execute(SQLConstants.CREATE_FILE_SQL);
      stmt.execute(SQLConstants.CREATE_CHUNK_SQL);
      stmt.execute(SQLConstants.CREATE_PAGE_SQL);
      stmt.execute(SQLConstants.CREATE_FILE_SERIES_STAT_SQL);
      stmt.execute(SQLConstants.CREATE_CHUNK_SERIES_STAT_SQL);
      stmt.execute(SQLConstants.CREATE_PAGE_SERIES_STAT_SQL);
    } catch (SQLException e) {
      LOGGER.error("Create tables failed ", e);
    }
  }

  public List<String> getAllTsFilesNeedUpdating(Map<String, Long> allTsFiles) {
    String selectSQL =
        String.format("SELECT file_path, file_version FROM %s", SQLConstants.FILE_TABLE_NAME);
    String deleteSQL =
        String.format("DELETE FROM %s WHERE file_path=?", SQLConstants.FILE_TABLE_NAME);
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
        if (allTsFiles.get(filePath) != null && allTsFiles.get(filePath) == fileVersion) {
          allTsFiles.remove(filePath);
        }
      }
    } catch (SQLException e) {
      LOGGER.error("Get all matching tsfile paths error: ", e);
    }

    return new ArrayList<>(allTsFiles.keySet());
  }

  private void prepareStatements(Connection conn) throws SQLException {
    writeToChunkStmt =
        conn.prepareStatement(
            String.format("INSERT INTO %s VALUES (?,?,?,?)", SQLConstants.CHUNK_TABLE_NAME),
            Statement.RETURN_GENERATED_KEYS);
    writeToPageStmt =
        conn.prepareStatement(
            String.format("INSERT INTO %s VALUES (?,?,?,?)", SQLConstants.PAGE_TABLE_NAME),
            Statement.RETURN_GENERATED_KEYS);
    writeToChunkSeriesStmt =
        conn.prepareStatement(
            String.format(
                "INSERT INTO %s VALUES (?,?,?,?,?,?,?,?)",
                SQLConstants.CHUNK_SERIES_STAT_TABLE_NAME));
    writeToPageSeriesStmt =
        conn.prepareStatement(
            String.format(
                "INSERT INTO %s VALUES (?,?,?,?,?,?,?,?)",
                SQLConstants.PAGE_SERIES_STAT_TABLE_NAME));
  }

  public int writeToSeries(Path tsPath) {
    String querySQL =
        String.format("SELECT sid FROM %s WHERE ts_path=?", SQLConstants.SERIES_TABLE_NAME);
    String insertSQL = String.format("INSERT INTO %s VALUES (?,?)", SQLConstants.SERIES_TABLE_NAME);
    try (Connection conn = ds.getConnection();
        PreparedStatement queryStmt = conn.prepareStatement(querySQL);
        PreparedStatement insertStmt =
            conn.prepareStatement(insertSQL, Statement.RETURN_GENERATED_KEYS)) {
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
      LOGGER.error("Insert into series table error: ", e);
    }
    return -1;
  }

  public int writeToFile(String filePath, long version) {
    String querySQL =
        String.format("SELECT fid FROM %s WHERE file_path=?", SQLConstants.FILE_TABLE_NAME);
    String insertSQL =
        String.format("INSERT OR IGNORE INTO %s VALUES (?,?,?,?)", SQLConstants.FILE_TABLE_NAME);
    try (Connection conn = ds.getConnection();
        PreparedStatement queryStmt = conn.prepareStatement(querySQL);
        PreparedStatement insertStmt =
            conn.prepareStatement(insertSQL, Statement.RETURN_GENERATED_KEYS)) {
      queryStmt.setString(1, filePath);
      ResultSet rs = queryStmt.executeQuery();
      if (rs.next()) {
        return rs.getInt(1);
      }

      insertStmt.setNull(1, Types.INTEGER);
      insertStmt.setLong(2, version);
      insertStmt.setString(3, filePath);
      insertStmt.setBoolean(4, false);
      insertStmt.executeUpdate();
      return insertStmt.getGeneratedKeys().getInt(1);
    } catch (SQLException e) {
      LOGGER.error("Insert into file table error: ", e);
    }
    return -1;
  }

  public int writeToChunk(PreparedStatement stmt, int fid, int sid, long offset) {
    try {
      stmt.setNull(1, Types.INTEGER);
      stmt.setInt(2, fid);
      stmt.setInt(3, sid);
      stmt.setLong(4, offset);
      stmt.executeUpdate();
      return stmt.getGeneratedKeys().getInt(1);
    } catch (SQLException e) {
      LOGGER.error("Insert into chunk table error: ", e);
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
      LOGGER.error("Insert into page table error: ", e);
    }
    return -1;
  }

  public synchronized void writeToChunkSeriesStat(
      PreparedStatement stmt, int cid, SeriesStat stat) {
    try {
      stmt.setInt(1, cid);
      stmt.setLong(2, stat.getStartTimestamp());
      stmt.setLong(3, stat.getEndTimestamp());
      stmt.setLong(4, stat.getCnt());
      stmt.setDouble(5, stat.getSum());
      stmt.setDouble(6, stat.getSquareSum());
      stmt.setDouble(7, stat.getMinValue());
      stmt.setDouble(8, stat.getMaxValue());
      stmt.executeUpdate();
    } catch (SQLException e) {
      LOGGER.error("Insert into chunk series stat table error: ", e);
    }
  }

  public synchronized void writeToPageSeriesStat(PreparedStatement stmt, int pid, SeriesStat stat) {
    try {
      stmt.setInt(1, pid);
      stmt.setLong(2, stat.getStartTimestamp());
      stmt.setLong(3, stat.getEndTimestamp());
      stmt.setLong(4, stat.getCnt());
      stmt.setDouble(5, stat.getSum());
      stmt.setDouble(6, stat.getSquareSum());
      stmt.setDouble(7, stat.getMinValue());
      stmt.setDouble(8, stat.getMaxValue());
      stmt.executeUpdate();
    } catch (SQLException e) {
      LOGGER.error("Insert into page series stat table error: ", e);
    }
  }

  public synchronized void writeToFileSeriesStat(int fid, int sid, SeriesStat stat) {
    String sql =
        String.format(
            "INSERT INTO %s VALUES (?,?,?,?,?,?,?,?,?)", SQLConstants.FILE_SERIES_STAT_TABLE_NAME);
    try (Connection conn = ds.getConnection();
        PreparedStatement stmt = conn.prepareStatement(sql)) {
      stmt.setInt(1, fid);
      stmt.setInt(2, sid);
      stmt.setLong(3, stat.getStartTimestamp());
      stmt.setLong(4, stat.getEndTimestamp());
      stmt.setLong(5, stat.getCnt());
      stmt.setDouble(6, stat.getSum());
      stmt.setDouble(7, stat.getSquareSum());
      stmt.setDouble(8, stat.getMinValue());
      stmt.setDouble(9, stat.getMaxValue());
      stmt.executeUpdate();
    } catch (SQLException e) {
      LOGGER.error("Insert into file series stat table error: ", e);
    }
  }

  public void writeOneChunkStat(int fid, int sid, FileSeriesStat stat) {
    try (Connection conn = ds.getConnection()) {
      conn.setAutoCommit(false);
      prepareStatements(conn);

      int cid = writeToChunk(writeToChunkStmt, fid, sid, stat.currentChunkOffset);
      writeToChunkSeriesStat(writeToChunkSeriesStmt, cid, stat.chunkStat);
      for (int i = 0; i < stat.pageStats.size(); i++) {
        SeriesStat pageStat = stat.pageStats.get(i);
        int pid = writeToPage(writeToPageStmt, cid, sid, i + 1);
        writeToPageSeriesStat(writeToPageSeriesStmt, pid, pageStat);
      }

      conn.commit();
    } catch (SQLException e) {
      LOGGER.error("Error writing one chunk statistic to sqlite: ", e);
    }
  }

  public void deleteAccordingToTsFileResources(List<TsFileResource> tsFileResources) {
    deleteAccordingToTsFilePaths(tsFileResources
        .stream()
        .map(x -> x.getTsFile().getAbsolutePath())
        .collect(Collectors.toList()));
  }

  public void deleteAccordingToTsFilePaths(List<String> tsFilePaths) {
    int n = tsFilePaths.size();
    String sql =
        String.format(
            "DELETE FROM %s WHERE file_path IN (%s)",
            SQLConstants.FILE_TABLE_NAME,
            String.join(",", Collections.nCopies(n, "?")));
    try (Connection conn = ds.getConnection();
        PreparedStatement stmt = conn.prepareStatement(sql)) {
      for (int i = 1; i <= n; i++) {
        stmt.setString(i, tsFilePaths.get(i-1));
      }
      stmt.executeUpdate();
    } catch (SQLException e) {
      LOGGER.error("Delete files error: ", e);
    }
  }

  public void deleteFromFileTable(String tsFilePath) {
    String sql =
        String.format("DELETE FROM %s WHERE file_path=?", SQLConstants.FILE_TABLE_NAME);
    try (Connection conn = ds.getConnection();
        PreparedStatement stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, tsFilePath);
      stmt.executeUpdate();
    } catch (SQLException e) {
      LOGGER.error("Delete file error: ", e);
    }
  }

  public String getReadFileSeriesSQL(Filter filter) {
    return String.format(
        "SELECT * FROM %s NATURAL JOIN "
            + "(SELECT sid FROM %s WHERE ts_path = ?) NATURAL JOIN "
            + "(SELECT fid, sid FROM %s %s)",
        SQLConstants.FILE_TABLE_NAME,
        SQLConstants.SERIES_TABLE_NAME,
        SQLConstants.FILE_SERIES_STAT_TABLE_NAME,
        filter == null ? "" : "WHERE " + filter.getSQLString());
  }

  public String getAggregateFileSeriesStatsSQL(Filter filter) {
    return String.format(
        "SELECT * FROM %s NATURAL JOIN "
            + "(SELECT sid FROM %s WHERE ts_path = ?) NATURAL JOIN "
            + "(SELECT * FROM %s %s)",
        SQLConstants.FILE_TABLE_NAME,
        SQLConstants.SERIES_TABLE_NAME,
        SQLConstants.FILE_SERIES_STAT_TABLE_NAME,
        filter == null ? "" : "WHERE " + filter.getSQLString());
  }

  public String getReadChunkSeriesSQL(Set<String> filePaths, Filter filter) {
    String baseSQL = String.format(
        "SELECT * FROM %s NATURAL JOIN "
            + "(SELECT cid FROM %s %s) NATURAL JOIN "
            + "(SELECT sid FROM %s WHERE ts_path = ?)",
        SQLConstants.CHUNK_TABLE_NAME,
        SQLConstants.CHUNK_SERIES_STAT_TABLE_NAME,
        filter == null ? "" : "WHERE " + filter.getSQLString(),
        SQLConstants.SERIES_TABLE_NAME);

    if (filePaths == null || filePaths.isEmpty()) {
      return baseSQL + String.format(" NATURAL JOIN (SELECT * FROM %s)", SQLConstants.FILE_TABLE_NAME);
    }

    String fileSQL = String.format("(%s)", String.join(",", Collections.nCopies(filePaths.size(), "?")));
    return String.format("%s NATURAL JOIN (SELECT * FROM %s WHERE file_path NOT IN %s)",
        baseSQL, SQLConstants.FILE_TABLE_NAME, fileSQL);
  }

  public String getAggregateChunkSeriesStatsSQL(
      Set<String> filePaths,
      Filter filter) {
    String baseSQL = String.format(
        "SELECT * FROM %s NATURAL JOIN "
            + "(SELECT sid FROM %s WHERE ts_path = ?) NATURAL JOIN "
            + "(SELECT * FROM %s %s)",
        SQLConstants.CHUNK_TABLE_NAME,
        SQLConstants.SERIES_TABLE_NAME,
        SQLConstants.CHUNK_SERIES_STAT_TABLE_NAME,
        filter == null ? "" : "WHERE " + filter.getSQLString());

    if (filePaths == null || filePaths.isEmpty()) {
      return baseSQL + String.format(" NATURAL JOIN (SELECT * FROM %s)", SQLConstants.FILE_TABLE_NAME);
    }

    String fileSQL = String.format("(%s)", String.join(",", Collections.nCopies(filePaths.size(), "?")));
    return String.format("%s NATURAL JOIN (SELECT * FROM %s WHERE file_path NOT IN %s)",
        baseSQL, SQLConstants.FILE_TABLE_NAME, fileSQL);
  }

  public String getAggregatePageSeriesStatsSQL(
      Set<String> filePaths,
      Set<Integer> chunkIDs,
      Filter filter) {
    String baseSQL = String.format(
        "SELECT * FROM %s NATURAL JOIN"
            + "(SELECT sid FROM %s WHERE ts_path=?) NATURAL JOIN "
            + "(SELECT * FROM %s %s)",
        SQLConstants.PAGE_TABLE_NAME,
        SQLConstants.SERIES_TABLE_NAME,
        SQLConstants.PAGE_SERIES_STAT_TABLE_NAME,
        filter == null ? "" : "WHERE " + filter.getSQLString());

    String fileSQL, chunkSQL;
    if (filePaths != null && !filePaths.isEmpty()) {
      fileSQL = String.format("(%s)", String.join(",", Collections.nCopies(filePaths.size(), "?")));
      baseSQL += String.format(" NATURAL JOIN (SELECT * FROM %s WHERE file_path NOT IN %s)",
          SQLConstants.FILE_TABLE_NAME, fileSQL);
    } else {
      baseSQL += String.format(" NATURAL JOIN (SELECT * FROM %s)", SQLConstants.FILE_TABLE_NAME);
    }
    if (chunkIDs != null && !chunkIDs.isEmpty()) {
      chunkSQL = String.format("(%s)", String.join(",", Collections.nCopies(chunkIDs.size(), "?")));
      baseSQL += String.format(" WHERE cid NOT IN %s", chunkSQL);
    }
    return baseSQL;
  }

  public Set<String> aggregateFileLevelStats(
      PartialPath seriesPath,
      Filter filter,
      Map<String, TsFileSeriesStat> tsFileSeriesStatMap) {
    return aggregateFileLevelStats(seriesPath.getFullPath(), filter, tsFileSeriesStatMap);
  }

  public Set<String> aggregateFileLevelStats(
      String seriesPath,
      Filter filter,
      Map<String, TsFileSeriesStat> tsFileSeriesStatMap) {
    Set<String> filePaths = new HashSet<>();
    String sql = getAggregateFileSeriesStatsSQL(filter);
    try (Connection conn = ds.getConnection();
         PreparedStatement stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, seriesPath);
      ResultSet rs = stmt.executeQuery();
      while (rs.next()) {
        String filePath = rs.getString("file_path");
        filePaths.add(filePath);
        TsFileSeriesStat stat = new TsFileSeriesStat();
        stat.setFileStat(new SeriesStat(rs));
        tsFileSeriesStatMap.put(filePath, stat);
      }
      return filePaths;
    } catch (SQLException e) {
      LOGGER.error("Error executing: \"" + sql + "\"" + e.getMessage());
    }
    return null;
  }

  public Set<Integer> aggregateChunkLevelStats(
      Set<String> filePaths,
      PartialPath seriesPath,
      Filter filter,
      Map<String, TsFileSeriesStat> tsFileSeriesStatMap) {
    return aggregateChunkLevelStats(
        filePaths,
        seriesPath.getFullPath(),
        filter,
        tsFileSeriesStatMap);
  }

  public Set<Integer> aggregateChunkLevelStats(
      Set<String> filePaths,
      String seriesPath,
      Filter filter,
      Map<String, TsFileSeriesStat> tsFileSeriesStatMap) {
    Set<Integer> chunkIDs = new HashSet<>();
    String sql = getAggregateChunkSeriesStatsSQL(filePaths, filter);
    try (Connection conn = ds.getConnection();
        PreparedStatement stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, seriesPath);
      if (filePaths != null && !filePaths.isEmpty()) {
        int i = 2;
        for (String filePath : filePaths) {
          stmt.setString(i++, filePath);
        }
      }
      ResultSet rs = stmt.executeQuery();
      while (rs.next()) {
        long offset = rs.getLong("offset");
        String filePath = rs.getString("file_path");
        chunkIDs.add(rs.getInt("cid"));
        TsFileSeriesStat tsFileSeriesStat = new TsFileSeriesStat();
        tsFileSeriesStat.setChunkStats(offset, new SeriesStat(rs));
        tsFileSeriesStatMap.put(filePath, tsFileSeriesStat);
      }
      return chunkIDs;
    } catch (SQLException e) {
      LOGGER.error("Error aggregating chunk-level statistics: " + e.getMessage());
    }
    return null;
  }

  public void aggregatePageLevelStats(
      Set<String> filePaths,
      Set<Integer> chunkIDs,
      PartialPath seriesPath,
      Filter filter,
      Map<String, TsFileSeriesStat> tsFileSeriesStatMap) {
    aggregatePageLevelStats(filePaths, chunkIDs, seriesPath.getFullPath(), filter, tsFileSeriesStatMap);
  }

  public void aggregatePageLevelStats(
      Set<String> filePaths,
      Set<Integer> chunkIDs,
      String seriesPath,
      Filter filter,
      Map<String, TsFileSeriesStat> tsFileSeriesStatMap) {
    String sql = getAggregatePageSeriesStatsSQL(filePaths, chunkIDs, filter);
    try (Connection conn = ds.getConnection();
        PreparedStatement stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, seriesPath);
      int i = 2;
      if (filePaths != null && !filePaths.isEmpty()) {
        for (String filePath : filePaths) {
          stmt.setString(i++, filePath);
        }
      }
      if (chunkIDs != null && !chunkIDs.isEmpty()) {
        for (int chunkID : chunkIDs) {
          stmt.setInt(i++, chunkID);
        }
      }
      ResultSet rs = stmt.executeQuery();
      while (rs.next()) {
        long offset = rs.getLong("offset");
        long timestamp = rs.getLong("start_timestamp");
        String filePath = rs.getString("file_path");
        TsFileSeriesStat tsFileSeriesStat = new TsFileSeriesStat();
        tsFileSeriesStat.setPageStats(offset, timestamp, new SeriesStat(rs));
        tsFileSeriesStatMap.put(filePath, tsFileSeriesStat);
      }
    } catch (SQLException e) {
      LOGGER.error("Error aggregating page-level statistics: " + e.getMessage());
    }
  }

  public Map<String, TsFileSeriesStat> aggregate(PartialPath seriesPath, Filter filter) {
    Map<String, TsFileSeriesStat> tsFileSeriesStatMap = new HashMap<>();
    Set<String> filePaths = aggregateFileLevelStats(seriesPath, filter, tsFileSeriesStatMap);
    Set<Integer> chunkIDs = aggregateChunkLevelStats(filePaths, seriesPath, filter, tsFileSeriesStatMap);
    aggregatePageLevelStats(filePaths, chunkIDs, seriesPath, filter, tsFileSeriesStatMap);
    return tsFileSeriesStatMap;
  }

  public Map<String, TsFileSeriesStat> getStatsUsed(PartialPath seriesPath, Filter filter) {
    return getStatsUsed(seriesPath.getFullPath(), filter);
  }

  public Map<String, TsFileSeriesStat> getStatsUsed(String seriesPath, Filter filter) {
    String readFileSeriesSQL = getReadFileSeriesSQL(filter);
    try (Connection conn = ds.getConnection();
         PreparedStatement readFileSeriesStmt = conn.prepareStatement(readFileSeriesSQL)) {
      Map<String, TsFileSeriesStat> res = new HashMap<>();

      readFileSeriesStmt.setString(1, seriesPath);
      ResultSet rs = readFileSeriesStmt.executeQuery();
      while (rs.next()) {
        TsFileSeriesStat stat = new TsFileSeriesStat();
        res.put(rs.getString("file_path"), stat);
      }

      String readChunkSeriesSQL = getReadChunkSeriesSQL(res.keySet(), filter);
      try (PreparedStatement readChunkSeriesStmt = conn.prepareStatement(readChunkSeriesSQL)) {
        readChunkSeriesStmt.setString(1, seriesPath);
        int i = 2;
        for (Map.Entry<String, TsFileSeriesStat> entry : res.entrySet()) {
          String filePath = entry.getKey();
          readChunkSeriesStmt.setString(i++, filePath);
        }
        rs = readChunkSeriesStmt.executeQuery();
        while (rs.next()) {
          String filePath = rs.getString("file_path");
          res.computeIfAbsent(filePath, x -> new TsFileSeriesStat());
          res.get(filePath).setChunkStats(rs.getLong("offset"), new SeriesStat(rs));
        }
        return res;
      }
    } catch (SQLException e) {
      LOGGER.error("Error getting all statistical used information: " + e.getMessage());
    }
    return null;
  }
}