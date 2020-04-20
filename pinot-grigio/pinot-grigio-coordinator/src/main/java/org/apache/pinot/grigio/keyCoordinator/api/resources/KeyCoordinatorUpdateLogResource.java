/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.grigio.keyCoordinator.api.resources;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.io.File;
import javax.ws.rs.Encoded;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.grigio.common.storageProvider.UpdateLogStorageProvider;


/**
 * APIs to get update logs for server bootstarp
 */
@Api(tags = "UpdateLog")
@Path("/")
public class KeyCoordinatorUpdateLogResource {

  @GET
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  @Path("/updatelog/{tableName}/{segmentName}")
  @ApiOperation(value = "Download update log", notes = "Download update log for a segment")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Success"), @ApiResponse(code = 500, message = "Internal error")})
  public Response downloadUpdateLog(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Name of the segment", required = true) @PathParam("segmentName") @Encoded String segmentName) {
    UpdateLogStorageProvider updateLogStorageProvider;
    try {
      updateLogStorageProvider = UpdateLogStorageProvider.getInstance();
    } catch (Exception e) {
      throw new WebApplicationException("Virtual column storage is not initialized");
    }

    String tableNameWithType =
        TableNameBuilder.ensureTableNameWithType(tableName, CommonConstants.Helix.TableType.REALTIME);
    File updateLogFile = updateLogStorageProvider.getSegmentFile(tableNameWithType, segmentName);
    if (!updateLogFile.exists()) {
      throw new WebApplicationException(
          "Update log for table: " + tableNameWithType + " segment: " + segmentName + " does not exist",
          Response.Status.NOT_FOUND);
    }

    Response.ResponseBuilder builder = Response.ok(updateLogFile);
    builder.header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + updateLogFile.getName());
    builder.header(HttpHeaders.CONTENT_LENGTH, updateLogFile.length());
    return builder.build();
  }
}
