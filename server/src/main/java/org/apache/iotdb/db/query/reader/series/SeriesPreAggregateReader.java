package org.apache.iotdb.db.query.reader.series;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.preaggregation.api.SeriesStat;
import org.apache.iotdb.db.engine.preaggregation.api.TsFileSeriesStat;
import org.apache.iotdb.db.engine.preaggregation.rdbms.RDBMS;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.filter.TsFileFilter;
import org.apache.iotdb.db.utils.FileLoaderUtils;
import org.apache.iotdb.tsfile.file.metadata.ITimeSeriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class SeriesPreAggregateReader extends SeriesReader {

  private static final RDBMS DB = RDBMS.getInstance();
  private final Map<String, TsFileSeriesStat> tsFileSeriesStats;
  private String currentTsFilePath;
  private long currentChunkOffset;

  public SeriesPreAggregateReader(
      PartialPath seriesPath,
      Set<String> allSensors,
      TSDataType dataType,
      QueryContext context,
      QueryDataSource dataSource,
      Filter timeFilter,
      Filter valueFilter,
      TsFileFilter fileFilter,
      boolean ascending) {
    super(
        seriesPath,
        allSensors,
        dataType,
        context,
        dataSource,
        timeFilter,
        valueFilter,
        fileFilter,
        ascending);
    tsFileSeriesStats = DB.getAllStats(seriesPath);
  }

  @Override
  public boolean hasNextFile() throws IOException {
    return super.hasNextFile();
  }

  @Override
  public void skipCurrentFile() {
    currentTsFilePath = "";
    super.skipCurrentFile();
  }

  @Override
  protected void unpackOneTimeSeriesMetadata(ITimeSeriesMetadata timeSeriesMetadata)
      throws IOException {
    super.unpackOneTimeSeriesMetadata(timeSeriesMetadata);
  }

  @Override
  protected void unpackSeqTsFileResource() throws IOException {
    TsFileResource tsFileResource = orderUtils.getNextSeqFileResource(true);
    ITimeSeriesMetadata timeSeriesMetadata =
        FileLoaderUtils.loadTimeSeriesMetadata(
            tsFileResource, seriesPath, context, getAnyFilter(), allSensors);
    if (timeSeriesMetadata != null) {
      currentTsFilePath = tsFileResource.getTsFile().getAbsolutePath();
      timeSeriesMetadata.setSeq(true);
      seqTimeSeriesMetadata.add(timeSeriesMetadata);
    }
  }

  @Override
  protected void unpackUnseqTsFileResource() throws IOException {
    TsFileResource tsFileResource = orderUtils.getNextUnseqFileResource(true);
    ITimeSeriesMetadata timeSeriesMetadata =
        FileLoaderUtils.loadTimeSeriesMetadata(
            tsFileResource, seriesPath, context, getAnyFilter(), allSensors);
    if (timeSeriesMetadata != null) {
      currentTsFilePath = tsFileResource.getTsFile().getAbsolutePath();
      timeSeriesMetadata.setModified(true);
      timeSeriesMetadata.setSeq(false);
      seqTimeSeriesMetadata.add(timeSeriesMetadata);
    }
  }

  private boolean containedByTimeFilter(Statistics statistics) {
    Filter timeFilter = getTimeFilter();
    return timeFilter == null
        || timeFilter.containStartEndTime(statistics.getStartTime(), statistics.getEndTime());
  }

  public boolean canUseCurrentFileStatistics() throws IOException {
    return !isFileOverlapped()
        && containedByTimeFilter(currentFileStatistics())
        && tsFileSeriesStats.get(currentTsFilePath) != null;
  }

  public SeriesStat currentFileSeriesStat() {
    return tsFileSeriesStats.get(currentTsFilePath).getFileStat();
  }

  public boolean canUseCurrentChunkStatistics() throws IOException {
    if (isChunkOverlapped() || !containedByTimeFilter(currentChunkStatistics())) {
      return false;
    }
    TsFileSeriesStat tsFileSeriesStat = tsFileSeriesStats.get(currentTsFilePath);
    if (tsFileSeriesStat == null) {
      return false;
    }
    return tsFileSeriesStat.getChunkStats().get(firstChunkMetadata.getOffsetOfChunkHeader())
        != null;
  }

  public SeriesStat currentChunkSeriesStat() {
    return tsFileSeriesStats
        .get(currentTsFilePath)
        .getChunkStats()
        .get(firstChunkMetadata.getOffsetOfChunkHeader());
  }

  @Override
  public boolean hasNextChunk() throws IOException {
    return super.hasNextChunk();
  }

  @Override
  public void skipCurrentChunk() {
    super.skipCurrentChunk();
  }

  public boolean hasNextPage() throws IOException {
    return super.hasNextPage();
  }

  public boolean canUseCurrentPageStatistics() throws IOException {
    Statistics currentPageStatistics = currentPageStatistics();
    if (currentPageStatistics == null) {
      return false;
    }
    return !isPageOverlapped()
        && containedByTimeFilter(currentPageStatistics)
        && !currentPageModified();
  }

  public Statistics currentPageStatistics() {
    return super.currentPageStatistics();
  }

  public void skipCurrentPage() {
    super.skipCurrentPage();
  }

  public BatchData nextPage() throws IOException {
    return super.nextPage().flip();
  }
}
