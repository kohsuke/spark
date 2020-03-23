/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network.server;

import com.google.common.base.Throwables;
import io.netty.channel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.protocol.*;

import static org.apache.spark.network.util.NettyUtils.getRemoteAddress;

/**
 * Base class for custom ChannelHandler for FetchRequest messages.
 * The messages can be processed in a separate event loop group or the one shared with other
 * {@link Message.Type}s. The behavior is decided by whether the config
 * `spark.shuffle.server.chunkFetchHandlerThreadsPercent` is set or not.
 * See more details in SPARK-30623.
 */
public abstract class ChunkFetchRequestHandler
    extends SimpleChannelInboundHandler<ChunkFetchRequest> {
  private static final Logger logger = LoggerFactory.getLogger(ChunkFetchRequestHandler.class);

  private final TransportClient client;
  private final StreamManager streamManager;
  private final long maxChunksBeingTransferred;

  protected ChunkFetchRequestHandler(
      TransportClient client,
      StreamManager streamManager,
      Long maxChunksBeingTransferred) {
    this.client = client;
    this.streamManager = streamManager;
    this.maxChunksBeingTransferred = maxChunksBeingTransferred;
  }

  public void processFetchRequest(
      final Channel channel, final ChunkFetchRequest msg) throws Exception {
    if (logger.isTraceEnabled()) {
      logger.trace("Received req from {} to fetch block {}", getRemoteAddress(channel),
        msg.streamChunkId);
    }
    long chunksBeingTransferred = streamManager.chunksBeingTransferred();
    if (chunksBeingTransferred >= maxChunksBeingTransferred) {
      logger.warn("The number of chunks being transferred {} is above {}, close the connection.",
        chunksBeingTransferred, maxChunksBeingTransferred);
      channel.close();
      return;
    }
    ManagedBuffer buf;
    try {
      streamManager.checkAuthorization(client, msg.streamChunkId.streamId);
      buf = streamManager.getChunk(msg.streamChunkId.streamId, msg.streamChunkId.chunkIndex);
      if (buf == null) {
        throw new IllegalStateException("Chunk was not found");
      }
    } catch (Exception e) {
      logger.error(String.format("Error opening block %s for request from %s",
        msg.streamChunkId, getRemoteAddress(channel)), e);
      respond(channel, new ChunkFetchFailure(msg.streamChunkId,
        Throwables.getStackTraceAsString(e)));
      return;
    }

    streamManager.chunkBeingSent(msg.streamChunkId.streamId);
    respond(channel, new ChunkFetchSuccess(msg.streamChunkId, buf)).addListener(
      (ChannelFutureListener) future -> streamManager.chunkSent(msg.streamChunkId.streamId));
  }

  public abstract ChannelFuture respond(Channel channel, Encodable result) throws Exception;

  @Override
  protected void channelRead0(
      ChannelHandlerContext channelHandlerContext,
      ChunkFetchRequest chunkFetchRequest) throws Exception { }
}
