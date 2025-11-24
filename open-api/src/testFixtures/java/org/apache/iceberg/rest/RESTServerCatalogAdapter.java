/*
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
package org.apache.iceberg.rest;

import java.util.Map;
import java.util.function.Consumer;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.rest.RESTCatalogServer.CatalogContext;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.util.PropertyUtil;

/**
 * RESTServerCatalogAdapter是RESTCatalogAdapter的扩展实现，专门用于服务端适配器。
 *
 * 主要功能：
 * 1. 在基础 RESTCatalogAdapter 功能之上，增加了凭证信息处理能力
 * 2. 当配置要求时，在响应中包含云存储访问凭证
 * 3. 支持多种云平台（AWS S3、GCP、Azure）的凭证传递
 */
class RESTServerCatalogAdapter extends RESTCatalogAdapter {
  // 配置键名，用于控制是否在响应中包含凭证信息
  private static final String INCLUDE_CREDENTIALS = "include-credentials";

  // 封装了后端 Catalog 实例和配置信息的上下文对象
  private final CatalogContext catalogContext;

  /**
   * 构造函数
   *
   * @param catalogContext 包含后端 Catalog 和配置信息的上下文对象
   */
  RESTServerCatalogAdapter(CatalogContext catalogContext) {
    // 调用父类构造函数，传入实际的 Catalog 实例
    super(catalogContext.catalog());
    this.catalogContext = catalogContext;
  }

  /**
   * 处理 REST 请求的核心方法，对父类处理结果进行增强
   *
   * @param route 路由信息
   * @param vars 路径变量
   * @param httpRequest HTTP 请求对象
   * @param responseType 响应类型
   * @param responseHeaders 响应头设置回调
   * @return 处理后的 REST 响应对象
   */
  @Override
  public <T extends RESTResponse> T handleRequest(
      Route route,
      Map<String, String> vars,
      HTTPRequest httpRequest,
      Class<T> responseType,
      Consumer<Map<String, String>> responseHeaders) {
    T restResponse = super.handleRequest(route, vars, httpRequest, responseType, responseHeaders);

    // 如果是加载表的响应，并且配置要求包含凭证信息
    if (restResponse instanceof LoadTableResponse) {
      if (PropertyUtil.propertyAsBoolean(
          catalogContext.configuration(), INCLUDE_CREDENTIALS, false)) {
        applyCredentials(
            catalogContext.configuration(), ((LoadTableResponse) restResponse).config());
      }
    }

    return restResponse;
  }

  /**
   * 将云存储凭证从 Catalog 配置应用到表配置中
   *
   * 支持的云平台凭证：
   * - AWS S3: 访问密钥 ID、秘密访问密钥、会话令牌
   * - GCP: OAuth2 令牌
   * - Azure: SAS 令牌、连接字符串
   *
   * @param catalogConfig Catalog 级别的配置，可能包含云存储凭证
   * @param tableConfig 表级别的配置，将被注入凭证信息
   */
  private void applyCredentials(
      Map<String, String> catalogConfig, Map<String, String> tableConfig) {
    if (catalogConfig.containsKey(S3FileIOProperties.ACCESS_KEY_ID)) {
      tableConfig.put(
          S3FileIOProperties.ACCESS_KEY_ID, catalogConfig.get(S3FileIOProperties.ACCESS_KEY_ID));
    }

    if (catalogConfig.containsKey(S3FileIOProperties.SECRET_ACCESS_KEY)) {
      tableConfig.put(
          S3FileIOProperties.SECRET_ACCESS_KEY,
          catalogConfig.get(S3FileIOProperties.SECRET_ACCESS_KEY));
    }

    if (catalogConfig.containsKey(S3FileIOProperties.SESSION_TOKEN)) {
      tableConfig.put(
          S3FileIOProperties.SESSION_TOKEN, catalogConfig.get(S3FileIOProperties.SESSION_TOKEN));
    }

    if (catalogConfig.containsKey(GCPProperties.GCS_OAUTH2_TOKEN)) {
      tableConfig.put(
          GCPProperties.GCS_OAUTH2_TOKEN, catalogConfig.get(GCPProperties.GCS_OAUTH2_TOKEN));
    }

    catalogConfig.entrySet().stream()
        .filter(
            entry ->
                entry.getKey().startsWith(AzureProperties.ADLS_SAS_TOKEN_PREFIX)
                    || entry.getKey().startsWith(AzureProperties.ADLS_CONNECTION_STRING_PREFIX))
        .forEach(entry -> tableConfig.put(entry.getKey(), entry.getValue()));
  }
}
