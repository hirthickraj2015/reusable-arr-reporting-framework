# Reusable ARR Reporting Framework

[![Python Version](https://img.shields.io/badge/python-3.7%2B-blue)](#)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](#)

---

## 🔍 What is this?

A Python-based framework for **fast**, **reusable**, and **configurable** ARR bucket creation and reporting tasks.

ARR = *Annual Recurring Revenue*. In many subscription / SaaS businesses, it’s critical to segment customers (or revenue) into buckets (e.g. <$10k, $10-50k, >$50k), compute growth, detect churn, etc.

This framework helps you:

* Define ARR buckets via simple configuration (no hardcoding),
* Run sanity checks, pre-processing and data quality validations,
* Produce reports based on those buckets in a consistent way,
* Reuse pieces across projects/pipelines, reducing duplication,
* Scale to large datasets using PySpark.

---

## 🛠 Why this repo exists / What problem it solves

Without a framework, teams often build ad-hoc scripts for ARR reporting. This leads to:

* Duplication of logic,
* Inconsistent results,
* Maintenance pain,
* Hard to scale or onboard new members.

This repo solves these by providing:

1. A **config file** + parameterization for buckets, etc.
2. A standard pipeline: *data pre-checks → pre-processing → bucket creation → checks → reporting*
3. Modular code for reusability and extension.
4. Data quality tests/checks to ensure reliability.

---

## 🧭 Repo structure & roles

| File/module              | Purpose                                                            |
| ------------------------ | ------------------------------------------------------------------ |
| `config.yaml`            | Configurable settings: bucket definitions, thresholds, paths, etc. |
| `main.py`                | Entry point: orchestrates the full pipeline                        |
| `install.py`             | Setup dependencies and environment checks                          |
| `data_pre_checks.py`     | Early validation of input data                                     |
| `data_pre_processing.py` | Data cleaning, formatting, type casting, etc.                      |
| `crb_checks.py`          | Business rules and bucket checks                                   |
| `crb_functions.py`       | Core logic for ARR bucket computation                              |
| `generic_tools.py`       | Utility functions: logging, error handling, Spark session setup    |
| `README.md`              | Documentation                                                      |

---

## 🚀 Getting Started

### Prerequisites

* Python 3.7+
* PySpark
* Spark cluster or local Spark
* Access to source data
* Permissions to write output reports

### Setup

1. **Clone the repository**

```bash
git clone https://github.com/hirthickraj2015/reusable-arr-reporting-framework.git
cd reusable-arr-reporting-framework
```

2. **Create/activate virtual environment**

```bash
python3 -m venv venv
source venv/bin/activate
```

3. **Install dependencies**

```bash
pip install -r requirements.txt
```

4. **Modify configuration**

Open `config.yaml` to define:

* Bucket ranges
* Input/output paths
* Logging/Spark settings

5. **Run pre-checks**

```bash
python main.py --step data_pre_checks
```

6. **Run full pipeline**

```bash
python main.py
```

7. **View output**

* Logs and reports in output folder
* Verify sample records for accuracy

---

## 🔄 Example flow

* Compute ARR per customer
* Assign to buckets (e.g. <10k, 10k-50k, >50k)
* Run CRB checks for outliers and aggregation validation
* Generate reports

---

## ⚙ How to Extend

* Add new buckets in `config.yaml`
* Add checks in `crb_checks.py`
* Support new dimensions (product, region, sales-rep)
* Change output formats (CSV, Parquet, JSON)
* Modularize for pipelines (daily/weekly runs)

---

## ✅ Best Practices & Tips

* Version bucket definitions
* Include schema expectations
* Informative logging
* Alerts for pre-check failures
* Idempotent runs
* Unit and integration tests
* Monitor Spark performance

---

## 🚧 Production Considerations

* Deployment: containerize or orchestrate via Airflow/Prefect
* Scheduling: daily/weekly/monthly
* Monitoring: run times, failures, data skew
* Security: manage secrets and data access
* Data lineage: track source and transformations

---

## 📄 Contribute

* Fork → feature branch → commit → test → pull request → review

---

## ⚙ Configuration Reference

| Key                        | Purpose                      |
| -------------------------- | ---------------------------- |
| `buckets`                  | ARR bucket ranges            |
| `input_path`/`output_path` | Data locations               |
| `checks`                   | Business/data quality checks |
| `spark_config`             | Spark options                |
| `logging`                  | Logging level, output format |

---

## 📩 FAQ

* Boundary handling: define inclusive/exclusive in config
* ARR definition changes: version configs and maintain historical runs
* Schema changes: caught by pre-checks, update code/config accordingly

---

## 📦 License & Acknowledgments

* MIT License

---

## 🎉 Tips

* Check logs first
* Run data_pre_checks
* Test small datasets for bucket logic


