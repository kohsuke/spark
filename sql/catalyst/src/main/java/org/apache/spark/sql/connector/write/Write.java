package org.apache.spark.sql.connector.write;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;

/**
 * A logical representation of a data source write.
 * <p>
 * This logical representation is shared between batch and streaming write. Data sources must
 * implement the corresponding methods in this interface to match what the table promises
 * to support. For example, {@link #toBatch()} must be implemented if the {@link Table} that
 * creates this {@link Write} returns {@link TableCapability#BATCH_WRITE} support in its
 * {@link Table#capabilities()}.
 */
public interface Write {

  default String description() {
    return this.getClass().toString();
  }

  /**
   * Returns a {@link BatchWrite} to write data to batch source. By default this method throws
   * exception, data sources must overwrite this method to provide an implementation, if the
   * {@link Table} that creates this write returns {@link TableCapability#BATCH_WRITE} support in
   * its {@link Table#capabilities()}.
   */
  default BatchWrite toBatch() {
    throw new UnsupportedOperationException(description() + ": Batch write is not supported");
  }

  /**
   * Returns a {@link StreamingWrite} to write data to streaming source. By default this method
   * throws exception, data sources must overwrite this method to provide an implementation, if the
   * {@link Table} that creates this write returns {@link TableCapability#STREAMING_WRITE} support
   * in its {@link Table#capabilities()}.
   */
  default StreamingWrite toStreaming() {
    throw new UnsupportedOperationException(description() + ": Streaming write is not supported");
  }
}
