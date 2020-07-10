package org.apache.hive.service.cli.operation;

import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.session.HiveSession;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;

public class OperationManagerMock extends OperationManager {
  private Set<OperationHandle> calledHandles = new HashSet<>();

  @Override
  public GetCatalogsOperation newGetCatalogsOperation(HiveSession parentSession) {
    GetCatalogsOperationMock operation = new GetCatalogsOperationMock(parentSession);
    try {
      Method m = OperationManager.class.getDeclaredMethod("addOperation", Operation.class);
      m.setAccessible(true);
      m.invoke(this, operation);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
    return operation;
  }

  @Override
  public void closeOperation(OperationHandle opHandle) throws HiveSQLException {
    calledHandles.add(opHandle);
    throw new RuntimeException();
  }

  public Set<OperationHandle> getCalledHandles() {
    return calledHandles;
  }
}
