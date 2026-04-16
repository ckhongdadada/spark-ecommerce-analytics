package com.ecommerce.module

import com.ecommerce.config.AppConfig
import com.ecommerce.warehouse.DataWarehouseBuilder
import com.ecommerce.util.Logging

/**
 * Module 7: Data Warehouse
 * 
 * 构建完整的数据仓库分层架构：
 * - ODS: 操作数据层
 * - DWD: 明细数据层
 * - DWS: 汇总数据层
 * - ADS: 应用数据层
 */
object Module7_DataWarehouse extends Logging {

  def run(): Unit = {
    logger.info("=" * 60)
    logger.info(s"  ${AppConfig.moduleName("7")}")
    logger.info("=" * 60)

    DataWarehouseBuilder.buildWarehouse()

    logger.info("Module 7 completed")
  }
}
