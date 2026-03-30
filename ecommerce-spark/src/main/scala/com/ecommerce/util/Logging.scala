package com.ecommerce.util

import org.slf4j.{Logger, LoggerFactory}

/**
 * 日志工具特质
 * 使用方式：class MyClass extends Logging { logger.info("message") }
 */
trait Logging {
  @transient private var _logger: Logger = _

  protected def logger: Logger = {
    if (_logger == null) {
      _logger = LoggerFactory.getLogger(getClass.getName)
    }
    _logger
  }
}
