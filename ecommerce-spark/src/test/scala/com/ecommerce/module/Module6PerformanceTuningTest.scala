package com.ecommerce.module

import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test

class Module6PerformanceTuningTest {

  @Test
  def summarizeMeasurementsShouldComputeMedianAndP95(): Unit = {
    val measurements = Seq(
      Module6_PerformanceTuning.BenchmarkMeasurement("join_strategy", "broadcast_join", "warmup", 1, 90L, 100L, 4, "2026-04-07 10:00:00"),
      Module6_PerformanceTuning.BenchmarkMeasurement("join_strategy", "broadcast_join", "measured", 1, 100L, 100L, 4, "2026-04-07 10:00:01"),
      Module6_PerformanceTuning.BenchmarkMeasurement("join_strategy", "broadcast_join", "measured", 2, 110L, 100L, 4, "2026-04-07 10:00:02"),
      Module6_PerformanceTuning.BenchmarkMeasurement("join_strategy", "broadcast_join", "measured", 3, 160L, 100L, 4, "2026-04-07 10:00:03")
    )

    val summary = Module6_PerformanceTuning.summarizeMeasurements(measurements).head

    assertEquals("join_strategy", summary.scenario)
    assertEquals("broadcast_join", summary.variant)
    assertEquals(3, summary.measuredRuns)
    assertEquals(123L, summary.avgDurationMs)
    assertEquals(110L, summary.medianDurationMs)
    assertEquals(160L, summary.p95DurationMs)
    assertEquals(26L, summary.stddevDurationMs)
    assertEquals(21.0811, summary.cvPercent, 0.0001)
    assertTrue(summary.cvPercent > 0.0)
  }
}
