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
package org.apache.spark.network.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.ClosedChannelException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ByteArrayReadableChannelTest {

  @Test
  public void testFeedDataMultipleBuffers() throws IOException {
    ByteArrayReadableChannel channel = new ByteArrayReadableChannel();
    ByteBuf first = Unpooled.wrappedBuffer(new byte[]{1, 2});
    ByteBuf second = Unpooled.wrappedBuffer(new byte[]{3, 4});
    channel.feedData(first);
    try {
      channel.feedData(second);
      fail();
    } catch (IllegalStateException expected) {

    }

    ByteBuffer dst = ByteBuffer.allocate(2);
    assertEquals(2, channel.read(dst));

    dst.flip();
    byte[] array = new byte[2];
    dst.get(array);

    assertArrayEquals(new byte[]{1, 2}, array);
    assertEquals(1, first.refCnt());
    assertEquals(1, second.refCnt());

    closeChannelAndAssertState(channel);

    assertTrue(first.release());
    assertTrue(second.release());
  }

  @Test
  public void testFeedDataAfterClosed() throws IOException {
    ByteArrayReadableChannel channel = new ByteArrayReadableChannel();
    closeChannelAndAssertState(channel);
    ByteBuf first = Unpooled.wrappedBuffer(new byte[]{ 1, 2 });
    try {
      channel.feedData(first);
      fail();
    } catch (ClosedChannelException expected) {

    }
    assertEquals(1, first.refCnt());
    assertTrue(first.release());
  }

  private static void closeChannelAndAssertState(Channel channel) throws IOException {
    assertTrue(channel.isOpen());
    channel.close();

    // Ensure we can call close multiple times
    channel.close();
    assertFalse(channel.isOpen());
  }
}
