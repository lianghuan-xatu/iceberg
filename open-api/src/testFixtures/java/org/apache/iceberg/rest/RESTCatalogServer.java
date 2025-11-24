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

import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.PropertyUtil;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RESTCatalogServer 是一个基于 Jetty 的 HTTP 服务器，用于提供 Iceberg REST Catalog API 服务。
 *
 * 该服务器的主要功能包括：
 * 1. 初始化后端 Iceberg Catalog（默认使用 JdbcCatalog）
 * 2. 启动 REST API 服务端点
 * 3. 处理来自客户端的 REST 请求
 *
 * 主要特性：
 * - 支持通过环境变量配置 Catalog 属性
 * - 默认使用内存 SQLite 数据库作为存储后端
 * - 自动创建临时仓库位置（如果未指定）
 * - 支持 GZIP 压缩传输
 * - 可配置的服务端口（默认 8181）
 */
public class RESTCatalogServer {

  // 日志记录器
  private static final Logger LOG = LoggerFactory.getLogger(RESTCatalogServer.class);

  // 服务端口配置键名
  public static final String REST_PORT = "rest.port";
  // 默认服务端口
  static final int REST_PORT_DEFAULT = 8181;

  // Catalog 名称配置键名
  public static final String CATALOG_NAME = "catalog.name";
  // 默认 Catalog 名称
  static final String CATALOG_NAME_DEFAULT = "rest_backend";

  // Jetty HTTP 服务器实例
  private Server httpServer;
  // 服务器配置属性映射
  private final Map<String, String> config;

  /**
   * 默认构造函数，创建空配置的服务器实例
   */
  RESTCatalogServer() {
    this.config = Maps.newHashMap();
  }

  /**
   * 带配置的构造函数
   *
   * @param config 服务器配置属性映射
   */
  RESTCatalogServer(Map<String, String> config) {
    this.config = config;
  }

  /**
   * 内部类，用于封装已初始化的 Catalog 实例及其配置
   */
  static class CatalogContext {
    private final Catalog catalog;
    private final Map<String, String> configuration;

    CatalogContext(Catalog catalog, Map<String, String> configuration) {
      this.catalog = catalog;
      this.configuration = configuration;
    }

    public Catalog catalog() {
      return catalog;
    }

    public Map<String, String> configuration() {
      return configuration;
    }
  }

  /**
   * 初始化后端 Iceberg Catalog
   *
   * 此方法会:
   * 1. 从环境变量和配置中获取 Catalog 属性
   * 2. 如果未指定 Catalog 实现，则默认使用 JdbcCatalog
   * 3. 如果未指定仓库位置，则创建临时目录
   * 4. 构建并返回 Catalog 实例
   *
   * @return 已初始化的 CatalogContext 对象
   * @throws IOException 如果创建临时目录失败
   */
  private CatalogContext initializeBackendCatalog() throws IOException {
    // Translate environment variables to catalog properties
    Map<String, String> catalogProperties = Maps.newHashMap(RCKUtils.environmentCatalogConfig());
    catalogProperties.putAll(config);

    // Fallback to a JDBCCatalog impl if one is not set
    catalogProperties.putIfAbsent(CatalogProperties.CATALOG_IMPL, JdbcCatalog.class.getName());
    catalogProperties.putIfAbsent(CatalogProperties.URI, "jdbc:sqlite::memory:");
    catalogProperties.putIfAbsent("jdbc.schema-version", "V1");

    // Configure a default location if one is not specified
    String warehouseLocation = catalogProperties.get(CatalogProperties.WAREHOUSE_LOCATION);

    if (warehouseLocation == null) {
      File tmp = java.nio.file.Files.createTempDirectory("iceberg_warehouse").toFile();
      tmp.deleteOnExit();
      warehouseLocation = new File(tmp, "iceberg_data").getAbsolutePath();
      catalogProperties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation);

      LOG.info("No warehouse location set. Defaulting to temp location: {}", warehouseLocation);
    }

    String catalogName =
        PropertyUtil.propertyAsString(catalogProperties, CATALOG_NAME, CATALOG_NAME_DEFAULT);

    LOG.info("Creating {} catalog with properties: {}", catalogName, catalogProperties);
    return new CatalogContext(
        CatalogUtil.buildIcebergCatalog(catalogName, catalogProperties, new Configuration()),
        catalogProperties);
  }

  /**
   * 启动 REST Catalog 服务器
   *
   * @param join 是否等待服务器线程结束
   * @throws Exception 如果启动过程中发生错误
   */
  public void start(boolean join) throws Exception {
    CatalogContext catalogContext = initializeBackendCatalog();

    RESTCatalogAdapter adapter = new RESTServerCatalogAdapter(catalogContext);
    RESTCatalogServlet servlet = new RESTCatalogServlet(adapter);

    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    ServletHolder servletHolder = new ServletHolder(servlet);
    context.addServlet(servletHolder, "/*");
    context.insertHandler(new GzipHandler());

    this.httpServer =
        new Server(
            PropertyUtil.propertyAsInt(catalogContext.configuration, REST_PORT, REST_PORT_DEFAULT));
    httpServer.setHandler(context);
    for (Connector connector : httpServer.getConnectors()) {
      ((ServerConnector) connector).setReusePort(true);
    }
    httpServer.start();

    if (join) {
      httpServer.join();
    }
  }

  /**
   * 停止 REST Catalog 服务器
   *
   * @throws Exception 如果停止过程中发生错误
   */
  public void stop() throws Exception {
    if (httpServer != null) {
      httpServer.stop();
    }
  }

  /**
   * 应用程序入口点
   *
   * @param args 命令行参数
   * @throws Exception 如果启动过程中发生错误
   */
  public static void main(String[] args) throws Exception {
    new RESTCatalogServer().start(true);
  }
}
