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
