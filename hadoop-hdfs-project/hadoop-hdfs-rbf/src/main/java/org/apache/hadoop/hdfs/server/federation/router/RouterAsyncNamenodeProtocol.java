/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.federation.router;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.federation.router.async.AsyncApplyFunction;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;

import java.io.IOException;
import java.util.Map;

import static org.apache.hadoop.hdfs.server.federation.router.async.AsyncUtil.asyncApply;
import static org.apache.hadoop.hdfs.server.federation.router.async.AsyncUtil.asyncComplete;
import static org.apache.hadoop.hdfs.server.federation.router.async.AsyncUtil.asyncReturn;

public class RouterAsyncNamenodeProtocol extends RouterNamenodeProtocol {

  public RouterAsyncNamenodeProtocol(RouterRpcServer server) {
    super(server);
  }

  @Override
  public BlocksWithLocations getBlocks(DatanodeInfo datanode, long size,
      long minBlockSize, long hotBlockTimeInterval, StorageType storageType) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    // Get the namespace where the datanode is located
    rpcServer.getDatanodeStorageReportMapAsync(HdfsConstants.DatanodeReportType.ALL, true, -1);
    asyncApply(o -> {
      Map<String, DatanodeStorageReport[]> map = (Map<String, DatanodeStorageReport[]>) o;
      String nsId = null;
      for (Map.Entry<String, DatanodeStorageReport[]> entry : map.entrySet()) {
        DatanodeStorageReport[] dns = entry.getValue();
        for (DatanodeStorageReport dn : dns) {
          DatanodeInfo dnInfo = dn.getDatanodeInfo();
          if (dnInfo.getDatanodeUuid().equals(datanode.getDatanodeUuid())) {
            nsId = entry.getKey();
            break;
          }
        }
        // Break the loop if already found
        if (nsId != null) {
          break;
        }
      }
      return nsId;
    });

    asyncApply((AsyncApplyFunction<String, BlocksWithLocations>) o -> {
      if (o != null) {
        String nsId = (String) o;
        // Forward to the proper namenode
        if (nsId != null) {
          RemoteMethod method = new RemoteMethod(
              NamenodeProtocol.class, "getBlocks",
              new Class<?>[] {DatanodeInfo.class, long.class, long.class},
              datanode, size, minBlockSize);
          rpcClient.invokeSingle(nsId, method, BlocksWithLocations.class);
        }
      } else {
        asyncComplete(null);
      }
    });
    return asyncReturn(BlocksWithLocations.class);
  }

  @Override
  public ExportedBlockKeys getBlockKeys() throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    RemoteMethod method =
        new RemoteMethod(NamenodeProtocol.class, "getBlockKeys");
    return rpcServer.invokeAtAvailableNsAsync(method, ExportedBlockKeys.class);
  }

  @Override
  public long getTransactionID() throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    RemoteMethod method =
        new RemoteMethod(NamenodeProtocol.class, "getTransactionID");
    return rpcServer.invokeAtAvailableNsAsync(method, long.class);
  }

  @Override
  public long getMostRecentCheckpointTxId() throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    RemoteMethod method =
        new RemoteMethod(NamenodeProtocol.class, "getMostRecentCheckpointTxId");
    rpcServer.invokeAtAvailableNsAsync(method, long.class);
    return asyncReturn(long.class);
  }

  @Override
  public NamespaceInfo versionRequest() throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    RemoteMethod method =
        new RemoteMethod(NamenodeProtocol.class, "versionRequest");
    rpcServer.invokeAtAvailableNsAsync(method, NamespaceInfo.class);
    return asyncReturn(NamespaceInfo.class);
  }
}
