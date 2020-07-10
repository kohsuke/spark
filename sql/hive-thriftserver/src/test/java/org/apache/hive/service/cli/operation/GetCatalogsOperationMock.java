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
package org.apache.hive.service.cli.operation;

import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.session.HiveSession;
import org.apache.hive.service.rpc.thrift.THandleIdentifier;
import org.apache.hive.service.rpc.thrift.TOperationHandle;
import org.apache.hive.service.rpc.thrift.TOperationType;

import java.nio.ByteBuffer;
import java.util.UUID;

public class GetCatalogsOperationMock extends GetCatalogsOperation {
  protected GetCatalogsOperationMock(HiveSession parentSession) {
    super(parentSession);
  }

  @Override
  public void runInternal() throws HiveSQLException {

  }

  public OperationHandle getHandle() {
    UUID uuid = UUID.randomUUID();
    THandleIdentifier tHandleIdentifier = new THandleIdentifier();
    tHandleIdentifier.setGuid(getByteBufferFromUUID(uuid));
    tHandleIdentifier.setSecret(getByteBufferFromUUID(uuid));
    TOperationHandle tOperationHandle = new TOperationHandle();
    tOperationHandle.setOperationId(tHandleIdentifier);
    tOperationHandle.setOperationType(TOperationType.GET_TYPE_INFO);
    tOperationHandle.setHasResultSetIsSet(false);
    return new OperationHandle(tOperationHandle);
  }

  private byte[] getByteBufferFromUUID(UUID uuid) {
    ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
    bb.putLong(uuid.getMostSignificantBits());
    bb.putLong(uuid.getLeastSignificantBits());
    return bb.array();
  }
}
