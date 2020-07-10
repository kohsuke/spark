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
    return new OperationHandle(new TOperationHandle(tHandleIdentifier, TOperationType.GET_TYPE_INFO, false));
  }

  private byte[] getByteBufferFromUUID(UUID uuid) {
    ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
    bb.putLong(uuid.getMostSignificantBits());
    bb.putLong(uuid.getLeastSignificantBits());
    return bb.array();
  }
}
