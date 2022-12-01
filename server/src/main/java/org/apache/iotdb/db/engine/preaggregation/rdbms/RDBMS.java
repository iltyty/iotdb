package org.apache.iotdb.db.engine.preaggregation.rdbms;

import org.apache.iotdb.db.engine.preaggregation.api.FileSeriesStat;
import org.apache.iotdb.db.engine.preaggregation.api.SeriesStat;
import org.apache.iotdb.tsfile.read.common.Path;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

import java.sql.*;
import java.util.*;

public class RDBMS {
  private final Logger LOGGER = LoggerFactory.getLogger(RDBMS.class);
  private final DataSource ds;

  private PreparedStatement querySeriesStmt;
  private PreparedStatement writeToSeriesStmt;
  private PreparedStatement writeToFileStmt;
  private PreparedStatement writeToChunkStmt;
  private PreparedStatement writeToPageStmt;
  private PreparedStatement writeToFileSeriesStmt;
  private PreparedStatement writeToChunkSeriesStmt;
  private PreparedStatement writeToPageSeriesStmt;

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
    querySeriesStmt =
        conn.prepareStatement(
            String.format("SELECT sid FROM %s WHERE ts_path=?", SQLConstants.SERIES_TABLE_NAME));
    writeToSeriesStmt =
        conn.prepareStatement(
            String.format("INSERT OR IGNORE INTO %s VALUES (?,?)", SQLConstants.SERIES_TABLE_NAME),
            Statement.RETURN_GENERATED_KEYS);
    writeToFileStmt =
        conn.prepareStatement(
            String.format(
                "INSERT OR IGNORE INTO %s VALUES (?,?,?,?)", SQLConstants.FILE_TABLE_NAME),
            Statement.RETURN_GENERATED_KEYS);
    writeToChunkStmt =
        conn.prepareStatement(
            String.format("INSERT INTO %s VALUES (?,?,?,?)", SQLConstants.CHUNK_TABLE_NAME),
            Statement.RETURN_GENERATED_KEYS);
    writeToPageStmt =
        conn.prepareStatement(
            String.format("INSERT INTO %s VALUES (?,?,?,?)", SQLConstants.PAGE_TABLE_NAME),
            Statement.RETURN_GENERATED_KEYS);
    writeToFileSeriesStmt =
        conn.prepareStatement(
            String.format(
                "INSERT INTO %s VALUES (?,?,?,?,?,?,?,?,?)",
                SQLConstants.FILE_SERIES_STAT_TABLE_NAME));
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
}
