package org.apache.iotdb.db.engine.preaggregation.rdbms;

public class SQLConstants {
  public static final String JDBC_URL = "jdbc:sqlite:/d:/preagg.db";
  public static final String CONN_TIMEOUT = "1000";
  public static final String MAX_SIZE = "100";
  public static final String DB_PREFIX = "preagg_";

  public static final String CLOSE_SYNC_SQL = "PRAGMA synchronous=OFF";
  public static final String FOREIGN_CONSTRAINT_SQL = "PRAGMA foreign_keys = ON";

  public static final String SERIES_TABLE_NAME = DB_PREFIX + "series";
  public static final String PAGE_TABLE_NAME = DB_PREFIX + "page";
  public static final String CHUNK_TABLE_NAME = DB_PREFIX + "chunk";
  public static final String FILE_TABLE_NAME = DB_PREFIX + "file";

  public static final String PAGE_SERIES_STAT_TABLE_NAME = DB_PREFIX + "page_series_stat";
  public static final String CHUNK_SERIES_STAT_TABLE_NAME = DB_PREFIX + "chunk_series_stat";
  public static final String FILE_SERIES_STAT_TABLE_NAME = DB_PREFIX + "file_series_stat";

  public static final String CREATE_SERIES_SQL =
      String.format(
          "CREATE TABLE IF NOT EXISTS %s(\n"
              + "    sid INTEGER PRIMARY KEY AUTOINCREMENT,\n"
              + "    ts_path VARCHAR(255) NOT NULL\n"
              + ");",
          SERIES_TABLE_NAME);
  public static final String CREATE_FILE_SQL =
      String.format(
          "CREATE TABLE IF NOT EXISTS %s(\n"
              + "    fid INTEGER PRIMARY KEY AUTOINCREMENT,\n"
              + "    file_version BIGINT NOT NULL,\n"
              + "    file_path VARCHAR(255) NOT NULL\n,"
              + "    is_updating BOOLEAN NOT NULL\n"
              + ");",
          FILE_TABLE_NAME);
  public static final String CREATE_CHUNK_SQL =
      String.format(
          "CREATE TABLE IF NOT EXISTS %s(\n"
              + "    cid INTEGER PRIMARY KEY AUTOINCREMENT,\n"
              + "    fid INT,\n"
              + "    sid INT,\n"
              + "    offset BIGINT,\n"
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
              + "    fid INT,\n"
              + "    sid INT,\n"
              + "    start_timestamp BIGINT NOT NULL,\n"
              + "    end_timestamp BIGINT NOT NULL,\n"
              + "    count BIGINT,\n"
              + "    sum DOUBLE,\n"
              + "    square_sum DOUBLE,\n"
              + "    min_value DOUBLE, \n"
              + "    max_value DOUBLE,\n"
              + "    PRIMARY KEY(fid, sid),\n"
              + "    FOREIGN KEY (fid) REFERENCES %s(fid) ON DELETE CASCADE,\n"
              + "    FOREIGN KEY (sid) REFERENCES %s(sid) ON DELETE CASCADE\n"
              + ");",
          FILE_SERIES_STAT_TABLE_NAME, FILE_TABLE_NAME, SERIES_TABLE_NAME);
  public static final String CREATE_CHUNK_SERIES_STAT_SQL =
      String.format(
          "CREATE TABLE IF NOT EXISTS %s(\n"
              + "    cid INTEGER PRIMARY KEY,\n"
              + "    start_timestamp BIGINT NOT NULL,\n"
              + "    end_timestamp BIGINT NOT NULL,\n"
              + "    count BIGINT,\n"
              + "    sum DOUBLE,\n"
              + "    square_sum DOUBLE,\n"
              + "    min_value DOUBLE,\n"
              + "    max_value DOUBLE,\n"
              + "    FOREIGN KEY (cid) REFERENCES %s(cid) ON DELETE CASCADE\n"
              + ");",
          CHUNK_SERIES_STAT_TABLE_NAME, CHUNK_TABLE_NAME);
  public static final String CREATE_PAGE_SERIES_STAT_SQL =
      String.format(
          "CREATE TABLE IF NOT EXISTS %s(\n"
              + "    pid INTEGER PRIMARY KEY,\n"
              + "    start_timestamp BIGINT NOT NULL,\n"
              + "    end_timestamp BIGINT NOT NULL,\n"
              + "    count BIGINT,\n"
              + "    sum DOUBLE,\n"
              + "    square_sum DOUBLE,\n"
              + "    min_value DOUBLE,\n"
              + "    max_value DOUBLE,\n"
              + "    FOREIGN KEY (pid) REFERENCES %s(pid) ON DELETE CASCADE\n"
              + ");",
          PAGE_SERIES_STAT_TABLE_NAME, PAGE_TABLE_NAME);
}
