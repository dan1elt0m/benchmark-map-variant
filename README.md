# Benchmark MapType vs VariantType in Apache Spark   

## Overview
SparkBenchmark is a Scala-based project designed to benchmark the performance of different data access methods in Apache Spark. The project compares the performance of `MapType` and `VariantType` data structures by measuring the time taken to access data.

## Prerequisites
- Scala 2.13.14
- SBT (Scala Build Tool)
- Apache Spark 4.0.0-preview2

## Setup

1. Build the project using SBT:
    ```sh
    sbt compile
    ```

2. Run the benchmarks:
    ```sh
    sbt run
    ```

## Project Structure
- `src/main/scala/Benchmark.scala`: Contains the main benchmarking code.
- `build.sbt`: SBT build configuration file.
- `src/main/resources/log4j2.properties`: Logging configuration.

## Benchmarking
The benchmarking process involves:
1. Generating synthetic data with varying numbers of keys.
2. Running benchmarks for both `MapType` and `VariantType` data structures.
3. Performing 3 iterations per benchmark and taking the median time.
4. Logging the progress and results.
5. Saving the results to a CSV file and generating a performance comparison chart.

## Results
The benchmark results are saved in `benchmark_results.csv` and a performance comparison chart is generated as `benchmark_performance_comparison.png`.
