# spark-etl Framework

## What is spark-etl?

`spark-etl` is a framework designed to simplify the development of Apache Spark ETL (Extract, Transform, Load) applications. It provides a structured approach with a focus on strong typing and composability, making it easier to build robust and maintainable data pipelines.

## Why is spark-etl a good match for strongly typed Spark applications?

Spark's Dataset API offers compile-time type safety, which helps catch errors early in the development process and improves code reliability. `spark-etl` leverages this by providing base classes and structures that encourage the use of Datasets throughout the pipeline. This makes your ETL logic:

*   **More Robust:** Type errors are caught during compilation, not at runtime on potentially large datasets.
*   **Easier to Refactor:** IDEs can better understand and assist with code changes.
*   **More Understandable:** Explicit types make the data flow and transformations clearer.

## Switching between Dataset and DataFrame in Production

While `spark-etl` encourages using the strongly-typed `Dataset` API during development for its safety benefits, Spark executes operations on DataFrames (essentially `Dataset[Row]`) for optimization purposes. The conversion between `Dataset[T]` and `DataFrame` is seamless in Spark.

*   **Dataset to DataFrame:** You can easily convert a `Dataset[T]` to a `DataFrame` using the `.toDF()` method.
*   **DataFrame to Dataset:** You can convert a `DataFrame` back to a `Dataset[T]` using the `.as[T]` method, provided the schema matches the case class `T`.

In a production environment, you might choose to work primarily with DataFrames for certain performance-critical sections or when dealing with less structured data. The framework allows flexibility, but promotes Datasets for core transformations where type safety adds significant value.

## Importing the Framework

To use `spark-etl` in your custom Spark project, you typically need to add it as a dependency.

**Using sbt:**

Add the following line to your `build.sbt` file (replace `VERSION` with the desired version):

```sbt
libraryDependencies += "com.your_organization" %% "spark-etl" % "VERSION"
```

*(Note: You'll need to publish the `spark-etl` framework artifact to a repository like Maven Central or a private Nexus/Artifactory instance for this to work.)*

**Using Maven:**

Add the following dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>com.your_organization</groupId>
    <artifactId>spark-etl_2.12</artifactId>  <!-- Adjust Scala version if needed -->
    <version>VERSION</version>
</dependency>
```

## Adding Memory for sbt Execution

When running Spark applications locally using `sbt run`, you might need to increase the memory allocated to the JVM.
You can do this by setting the `SBT_OPTS` environment variable or by creating a `.sbtopts` file in your project root:

**Using Environment Variable (Bash/Zsh):**

```bash
export SBT_OPTS="-Xms1G -Xmx4G -Xss2M"
sbt run
```

**Using `.sbtopts` file:**

Create a file named `.sbtopts` in your project's root directory and add:

```
-J-Xms1G
-J-Xmx4G
-J-Xss2M
```

Adjust the values (`-Xms` for initial heap, `-Xmx` for max heap, `-Xss` for thread stack size) based on your application's needs and available system memory.

## Creating Custom Components

`spark-etl` is designed to be extensible. Here's how to create custom components based on the examples in `spark-etl-examples`:

### Custom `TransformProcessor` (`SalesProcessor.scala`)

A `TransformProcessor` orchestrates the pipeline execution. It defines the pipeline, any post-pipeline transformations/filters, and the writer.

1.  **Extend `TransformProcessor`:** Specify input (`RawSale`), output (`Sale`), and configuration (`SalesConfig`) types.
2.  **Define Pipeline:** Instantiate your custom `TransformPipeline` (e.g., `SalesPipeline`).
3.  **Define Post-Steps (Optional):** Specify any `Transformer` or `Filter` to run after the main pipeline. Often, `Identity()` is used if no post-processing is needed.
4.  **Define Writer:** Specify how to write the final output (e.g., `Writer.DefaultParquetWriter()`).

```scala
package com.github.spark.etl.examples.processors

import com.github.spark.etl.core.app.processor.CustomEncoders.customEncoder // Ensure encoders are in scope
import com.github.spark.etl.core.app.processor.{Filter, TransformPipeline, TransformProcessor, Transformer, Writer}
import com.github.spark.etl.examples.config.SalesConfig
import com.github.spark.etl.examples.domain.{RawSale, Sale}
import com.github.spark.etl.examples.pipelines.SalesPipeline

// Requires an implicit config in scope when instantiated
class SalesProcessor(implicit config: SalesConfig)
  extends TransformProcessor[RawSale, Sale, SalesConfig] {

  // 1. Define the main pipeline
  lazy val pipeline: TransformPipeline[RawSale, Sale, SalesConfig] =
    new SalesPipeline() // Instantiate your custom pipeline

  // 2. Define post-pipeline transformer (Identity means no change)
  lazy val postTransformer: Transformer[Sale, Sale] =
    Transformer.Identity()

  // 3. Define post-pipeline filter (Identity means no filtering)
  lazy val postFilter: Filter[Sale, SalesConfig] =
    Filter.Identity()

  // 4. Define the writer
  lazy val writer: Writer[Sale] =
    Writer.DefaultParquetWriter() // Write results as Parquet
}
```

### Custom `TransformApplication` (`SalesApplication.scala`)

The `TransformApplication` is the main entry point. It extends a base application trait, wires up the configuration, and instantiates the `TransformProcessor`.

1.  **Extend `TransformApplication`:** Define it as a Scala `object`. Specify input, output, and config types.
2.  **Instantiate Processor:** Create an instance of your custom `TransformProcessor` (e.g., `SalesProcessor`). The framework handles configuration loading (using libraries like PureConfig) and invoking the processor.

```scala
package com.github.spark.etl.examples.application

import com.github.spark.etl.core.app.application.AbstractApplication.TransformApplication
import com.github.spark.etl.core.app.processor.TransformProcessor
import com.github.spark.etl.examples.config.SalesConfig
import com.github.spark.etl.examples.domain.{RawSale, Sale}
import com.github.spark.etl.examples.processors.SalesProcessor
import pureconfig.generic.auto._ // For automatic config derivation

object SalesApplication extends TransformApplication[RawSale, Sale, SalesConfig] {

  // Instantiate the specific processor for this application
  lazy val transformProcessor: TransformProcessor[RawSale, Sale, SalesConfig] =
    new SalesProcessor()
}
```

### Custom `TransformPipeline` (`SalesPipeline.scala`)

A `TransformPipeline` defines the core ETL flow: loading, pre-filtering, transforming, and post-filtering.

1.  **Extend `TransformPipeline`:** Specify input, output, and config types. Pass the input path source (e.g., from `config.app.source.input`) to the constructor.
2.  **Define Loader:** Specify how to load the initial data (e.g., `Loader.DefaultParquetLoader()`).
3.  **Define Pre-Filter (Optional):** Filter data *before* the main transformation (e.g., `Filter.Identity()`).
4.  **Define Transformer:** Instantiate your core `Transformer` (e.g., `RawSaleToSalesTransformer`).
5.  **Define Post-Filter (Optional):** Filter data *after* the main transformation (e.g., `SaleFilter`).

```scala
package com.github.spark.etl.examples.pipelines

import com.github.spark.etl.core.app.processor.CustomEncoders._ // Encoders needed
import com.github.spark.etl.core.app.processor.{Filter, Loader, TransformPipeline, Transformer}
import com.github.spark.etl.examples.config.SalesConfig
import com.github.spark.etl.examples.domain.{RawSale, Sale}
import com.github.spark.etl.examples.filters.SaleFilter
import com.github.spark.etl.examples.transformers.RawSaleToSalesTransformer

// Requires an implicit config
class SalesPipeline(implicit config: SalesConfig)
  extends TransformPipeline[RawSale, Sale, SalesConfig](
    config.app.source.input // Pass input path source
  ) {

  // 1. Define how to load data
  override lazy val loader: Loader[RawSale] =
    Loader.DefaultParquetLoader()

  // 2. Define filter before transformation (Identity = no filter)
  override lazy val preFilter: Filter[RawSale, SalesConfig] =
    Filter.Identity()

  // 3. Define the main data transformer
  override lazy val transformer: Transformer[RawSale, Sale] =
    new RawSaleToSalesTransformer()

  // 4. Define filter after transformation
  override lazy val postFilter: Filter[Sale, SalesConfig] =
    new SaleFilter()
}
```

### Custom `Transformer` (`RawSaleToSalesTransformer.scala`)

A `Transformer` converts a `Dataset` from one type to another. The `MappingTransformer` is a common base class.

1.  **Extend `MappingTransformer`:** Specify input (`RawSale`) and output (`Sale`) types.
2.  **Implement `mapping`:** (Optional but recommended for clarity/testing) Define a function that takes an input case class and returns an output case class.
3.  **Implement `mappingCols`:** Define a `ListMap` where keys are output column names and values are Spark `Column` expressions defining the transformation.

```scala
package com.github.spark.etl.examples.transformers

import com.github.spark.etl.core.app.processor.CustomEncoders.customEncoder
import com.github.spark.etl.core.app.processor.Transformer.MappingTransformer
import com.github.spark.etl.examples.domain.{RawSale, Sale}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, to_timestamp}

import java.sql.Timestamp
import scala.collection.immutable.ListMap

class RawSaleToSalesTransformer
  extends MappingTransformer[RawSale, Sale] {

  // Optional: Case-class level mapping logic
  override def mapping(input: RawSale): Sale =
    Sale(id = input.id,
         customerId = input.customerId,
         customerName = input.customerName,
         // ... other fields
         timestamp = Timestamp.valueOf(input.timestamp_str)
        )

  // Required: Column-level mapping for DataFrame transformation
  override def mappingCols(): ListMap[String, Column] =
    ListMap(
        Sale.idCol -> col(RawSale.idCol),
        Sale.customerIdCol -> col(RawSale.customerIdCol),
        // ... other direct mappings
        // Example transformation:
        Sale.timestampCol -> to_timestamp(col(RawSale.timestamp_strCol), "dd-MM-yyyy HH:mm")
    )
}
```

### Custom `Filter` (`SaleFilter.scala`)

A `Filter` selectively keeps rows in a `Dataset` based on a condition.

1.  **Extend `Filter`:** Specify the data type (`Sale`) and config type (`SalesConfig`).
2.  **Implement `filter`:** (Optional but recommended) Define a function that takes a case class instance and returns `true` if it should be kept.
3.  **Implement `filterColumn`:** Define a Spark `Column` expression that evaluates to `true` for rows to keep.

```scala
package com.github.spark.etl.examples.filters

import com.github.spark.etl.core.app.config.Env
import org.apache.spark.sql.functions._
import com.github.spark.etl.core.app.processor.Filter
import com.github.spark.etl.examples.config.SalesConfig
import com.github.spark.etl.examples.domain.Sale
import com.github.spark.etl.examples.utils.TimeUtils.firstDayOfMonth // Example utility
import org.apache.spark.sql.{Column, SparkSession}

class SaleFilter extends Filter[Sale, SalesConfig] {

  // Optional: Case-class level filter logic
  override def filter(implicit config: SalesConfig,
                       env: Env,
                       spark: SparkSession): Sale => Boolean =
    sale => sale.timestamp.after(firstDayOfMonth) // Keep sales after first day of month

  // Required: Column-level filter logic for DataFrames
  override def filterColumn(implicit config: SalesConfig,
                             env: Env,
                             spark: SparkSession): Column =
    col(Sale.timestampCol) > lit(firstDayOfMonth)
}
```

### Example: Composing Filters and Transformers in a Pipeline

Here's an example of a `Pipeline` that demonstrates the composition of `Filter` and `Transformer` components, similar to the `AccessoriesAnalyticsPipeline` you might be familiar with. This example conceptualizes an application log analysis pipeline.

```scala
package com.github.spark.etl.examples.pipelines

import com.github.spark.etl.core.app.TransformPipeline
import com.github.spark.etl.core.loader.Loader
import com.github.spark.etl.core.app.processor.{Filter, Transformer}
import com.github.spark.etl.core.app.processor.TransformerImplicits._
import com.github.spark.etl.core.app.processor.FilterImplicits._

// --- Hypothetical Configuration and Data Models ---
// (Adapt with your own Config, RawLog, ProcessedLog, LogSummary classes)

// Configuration (similar to SalesConfig)
trait AppSpecificConfig {
  def app: AppSubConfig
  // Other specific configurations if needed
  def minSeverityLevel: Int
  def targetSubSystem: String
}
trait AppSubConfig {
  def source: SourceSubConfig
}
trait SourceSubConfig {
  def input: String
}
case class LogAnalysisConfig(
  inputPath: String,
  override val minSeverityLevel: Int,
  override val targetSubSystem: String
) extends AppSpecificConfig {
  override def app: AppSubConfig = new AppSubConfig {
    override def source: SourceSubConfig = new SourceSubConfig {
      override def input: String = inputPath
    }
  }
}

// Data Models
case class RawLog(timestamp: Long, level: Int, subsystem: String, message: String, rawDetails: String)
case class ProcessedLog(timestamp: Long, level: Int, subsystem: String, message: String, importantInfo: Option[String], isCritical: Boolean)
case class LogSummary(subsystem: String, criticalCount: Int, warningCount: Int, mostFrequentMessage: Option[String])

// --- Hypothetical Filters (implementing Filter[T, ConfigType]) ---
// (You would define the actual filtering logic in these classes)

// Filter to keep only logs of a certain severity level or higher
class SeverityFilter(implicit config: LogAnalysisConfig) extends Filter[RawLog, LogAnalysisConfig] {
  override def apply(data: RawLog): Boolean = data.level >= config.minSeverityLevel
  // The `process(dataset: Dataset[RawLog]): Dataset[RawLog]` method would be implemented for Spark
}

// Filter to keep only logs from a specific subsystem
class SubSystemFilter(implicit config: LogAnalysisConfig) extends Filter[RawLog, LogAnalysisConfig] {
  override def apply(data: RawLog): Boolean = data.subsystem == config.targetSubSystem
}

// Filter to keep only summaries with a significant number of critical alerts
class CriticalSummaryFilter(implicit config: LogAnalysisConfig) extends Filter[LogSummary, LogAnalysisConfig] {
  override def apply(data: LogSummary): Boolean = data.criticalCount > 5
}

// --- Hypothetical Transformers (implementing Transformer[InputType, OutputType]) ---
// (You would define the actual transformation logic)

// Transformer to parse raw details and identify important information
class DetailParserTransformer(implicit config: LogAnalysisConfig) extends Transformer[RawLog, ProcessedLog] {
  override def apply(raw: RawLog): ProcessedLog = {
    // Parsing and extraction logic (simple example)
    val important = if (raw.rawDetails.contains("ERROR_CODE")) Some(raw.rawDetails) else None
    val critical = raw.level >= 5 // Assume 5+ is critical
    ProcessedLog(raw.timestamp, raw.level, raw.subsystem, raw.message, important, critical)
  }
}

// Transformer to enrich the log, e.g., by marking repetitive messages (simplified)
class EnrichmentTransformer(implicit config: LogAnalysisConfig) extends Transformer[ProcessedLog, ProcessedLog] {
  override def apply(log: ProcessedLog): ProcessedLog = {
    // Enrichment logic (example)
    log.copy(message = s"[ENRICHED] ${log.message}")
  }
}

// Transformer to aggregate processed logs into a summary per subsystem
// Note: An aggregation transformer typically operates on an entire Dataset.
// The `++` composition assumes the framework can chain transformers
// where the next one takes the output of the previous one.
class LogAggregatorTransformer(implicit config: LogAnalysisConfig) extends Transformer[ProcessedLog, LogSummary] {
  override def apply(log: ProcessedLog): LogSummary = {
    // This is a simplification for the `apply` example.
    // True aggregation would happen in `process(dataset: Dataset[ProcessedLog]): Dataset[LogSummary]`.
    LogSummary(
      subsystem = log.subsystem,
      criticalCount = if (log.isCritical) 1 else 0,
      warningCount = if (log.level == 4) 1 else 0, // Assume 4 = warning
      mostFrequentMessage = Some(log.message) // Extreme simplification
    )
  }
}

// --- Example Pipeline ---
class ApplicationLogAnalysisPipeline(implicit config: LogAnalysisConfig)
  extends TransformPipeline[RawLog, LogSummary, LogAnalysisConfig](
    config.app.source.input // Input path for source data
  ) {

  // 1. Define data loading (e.g., from CSV files)
  override lazy val loader: Loader[RawLog] =
    Loader.DefaultCsvLoader() // Assume a Loader.DefaultCsvLoader[RawLog]()

  // 2. Define filters BEFORE transformation (filter composition)
  //    - Keep logs with minimum severity
  //    - Keep logs for a target subsystem
  override lazy val preFilter: Filter[RawLog, LogAnalysisConfig] =
    new SeverityFilter() ++
    new SubSystemFilter()

  // 3. Define the main transformer (transformer composition)
  //    - Parse raw log details
  //    - Enrich log information
  //    - Aggregate logs to get summaries
  override lazy val transformer: Transformer[RawLog, LogSummary] =
    new DetailParserTransformer() ++      // RawLog => ProcessedLog
    new EnrichmentTransformer() ++    // ProcessedLog => ProcessedLog
    new LogAggregatorTransformer()    // ProcessedLog => LogSummary (aggregation)

  // 4. Define filters AFTER transformation (can also be a composition)
  //    - Keep summaries with a sufficient number of critical errors
  //    OR use Filter.Identity() if no post-transform filter is required.
  override lazy val postFilter: Filter[LogSummary, LogAnalysisConfig] =
    new CriticalSummaryFilter() ++
    Filter.by[LogSummary, LogAnalysisConfig](summary => summary.warningCount > 10) // Additional ad-hoc filter
}

// To use this pipeline:
// 1. Ensure base classes (TransformPipeline, Loader, Filter, Transformer)
//    and implicits (TransformerImplicits, FilterImplicits) exist and are imported.
// 2. Implement the actual logic in your filters and transformers (especially the `process` methods for Spark).
// 3. Provide an implicit instance of `LogAnalysisConfig`.
// 4. Run the pipeline, typically via a `run()` or `process()` method on the pipeline instance.

// Conceptual usage example:

// object MyApp {
//   def main(args: Array[String]): Unit = {
//     implicit val spark: SparkSession = SparkSession.builder().appName("LogAnalysis").master("local[*]").getOrCreate() // Required for Spark
//     implicit val config: LogAnalysisConfig = LogAnalysisConfig(
//       inputPath = "path/to/your/logs",
//       minSeverityLevel = 3, // e.g., WARNING and above
//       targetSubSystem = "payment-service"
//     )

//     val logPipeline = new ApplicationLogAnalysisPipeline()
//     logPipeline.run() // or an equivalent method to start the pipeline

//     spark.stop()
//   }
// }