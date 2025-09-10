# LpBound

![Paper](https://img.shields.io/badge/paper-arXiv:2502.05912-red) ![License](https://img.shields.io/badge/license-MIT-blue)

This repository contains the code used in the ACM SIGMOD 2025 paper 

[LpBound: Pessimistic Cardinality Estimation Using lp-norms of Degree Sequences](https://dl.acm.org/doi/10.1145/3725321). 

This paper introduces **LpBound**, a pessimistic cardinality estimator for multijoin queries (acyclic or cyclic) with selection predicates and group-by clauses. LpBound computes a guaranteed upper bound on the size of the query output using simple statistics on the input relations, i.e., *lp-norms of degree sequences*.

Check out the [website](https://www.ifi.uzh.ch/en/dast/research/LpBound.html) for more details.  If you have any questions about the code, please feel free to reach out to Christoph Mayer and Haozhe Zhang.

## Reproducing Experiments

This repository contains the information needed to reproduce the experimental results in the paper.

### Overview

1.  **Environment Setup:** Begin with the [Setup](#setup) section to install all prerequisites, compile the C++ modules, and download the datasets. 
2.  **Running the Experiments:** Proceed to the [Running Experiments](#running-experiments) section to execute the scripts that generate the experimental data used for the figures and tables in the paper. The scripts will generate the data in the `results/` directory. We have provided the data we generated for the paper in the `results/` directory. Re-running the experiments will overwrite the results.
3.  **Analyzing Results:** Use the Jupyter `notebooks/` to read the data in the `results/` directory and generate the figures and tables in the paper.
4.  **(Optional) Using Your Own Data:** For instructions on how to apply this framework to new datasets, see the [New Data and Queries](#new-data-and-queries) section.


### Core Experiments

The core experiments evaluate several performance and accuracy metrics across several standard query workloads.

| Metrics Evaluated | Workloads |
| :--- | :--- |
| Estimation Error<br>Estimation Time<br>Stats Precomputation Time<br>Space Requirement | `job-join`<br>`job-light`<br>`job-range`<br>`stats`<br>`subgraph-matching` |

### Detailed Analyses

Beyond the core results, the code also supports the following detailed analyses:

- Effectiveness on Group-By Queries:
  - Analyzes estimation errors on the `job-join-gby`, `job-light-gby`, `job-range-gby`, and `stats-gby` workloads.
- LP Optimization Effectiveness:
  - Measures the runtime impact of the `lp_berge` and `lp_flow` optimizations on the linear program.
- Parameter and Optimization Tuning:
  - Evaluates the effect of varying the number of Most Common Values (MCVs) on the estimation error.
  - Evaluates the impact of the Foreign-Key/Primary-Key (FKPK) and prefix optimizations.

## Program Structure

Our program is composed of several components that work together to estimate query cardinalities. Below is a diagram illustrating the workflow and the interaction between these components.

![LpBound Diagram](structure.svg)

**Stage 1: Precomputation of Statistics**

This offline stage generates statistical summaries from the database. As shown in the diagram, it uses the database schema (stored in `benchmarks/schemas` and see `src/lpbound/config/benchmark_schema.py`) and a configuration (see `src/lpbound/config/lpbound_config.py`) to create and execute SQL queries, storing the resulting statistics for use in the estimation stage.
The DuckDB version used in the paper is `0.10.1`.

**Stage 2: Cardinality Estimation**

This online stage provides a cardinality estimate for a given SQL query. It works by parsing the query, fetching the relevant statistics from Stage 1, and then constructing a Linear Program. A standard LP solver is used to solve this program and produce the final estimate.

**Implementation Versions**

The part of the program that is responsible for the cardinality estimation is implemented in two different ways (highlighted in the diagram): 
- **Pure Python Version**: This version is provided for ease of setup and use. You can use it to estimate the cardinalities of the queries in the benchmarks.
- **C++ Accelerated Version**: This version is implemented in C++ for a significant speed advantage. The performance results reported in our paper were generated using this version.

**Note**: If you plan to benchmark our approach, especially when comparing estimation time, you **must** use the C++ Accelerated Version. 
Our scripts use the correct version of the code to generate the results.
See Running Experiments section for more details.

**Note**: The code does not check the validity of the inputs or whether the queries are berge-acyclic, acyclic, or cyclic, or whether the queries are group-by queries. We rely on the user to call the correct functions to generate the results.
For reproducing the results of the paper, please use the scripts in the `benchmarks/experiments` directory.

## File Structure

The project is organized into several directories. Here is a high-level overview of the most important ones:

```bash
.
├── benchmarks/      # All assets to run experiments from the paper.
│  ├── experiments/  # Python scripts for each experiment (accuracy, time, etc.).
│  ├── schemas/      # JSON schema definitions for the databases.
│  └── workloads/    # SQL queries and metadata for each benchmark workload.
│
├── src/
│   └── lpbound/       # The core Python source code for the LpBound library.
│      ├── config/      # Main configuration files (e.g., lpbound_config.py).
│      ├── acyclic/    # Logic for acyclic queries (parsing, stats, LP construction).
│      ├── cyclic/     # Logic specific to cyclic queries.
│      ├── solver/     # Pure Python implementation of the LP formulation.
│      ├── cpp_solver/ # C++ implementation for the accelerated estimator.
│      ├── LpFlow/     # C++ implementation for the flow-based estimator.
│      └── utils/      # Shared utility functions.
│
├── notebooks/       # Jupyter notebooks for analysis and plotting results.
│
├── results/         # Contains the final experiment results (CSV files)
│
├── data/            # Scripts and instructions to download and set up datasets.
│
├── output/          # Default directory for intermediate generated files (e.g., SQL for stats).
│
└── pyproject.toml   # Project configuration and dependency management.
```

## New Data and Queries

If you want to use the code for your own data and queries, you need to:
1. Create a new schema file in the `benchmarks/schemas` directory. See the `benchmarks/schemas/joblight.json` file for an example.
2. Add the workloads to the `benchmarks/workloads` directory, which contains the SQL queries in a `.sql` file like `benchmarks/workloads/joblight/joblightQueries.sql`, (optional) the subqueries in a `.csv` file like `benchmarks/workloads/joblight/joblight_subqueries.csv` if you want to test the end-to-end evaluation time, and (optional) the metadata that describes the number of tables in the queries like `benchmarks/workloads/joblight/joblight_relation_counts.csv` if you want to generate more fine-grained boxplots.
3. Put the data in the `data/datasets` directory. Note that the code expects the files to be in lowercase.
4. Our code uses `DuckDB 0.10.1` to generate the statistics for the benchmark. You need to provide the SQL scripts to create the tables and insert the data into the tables. Add the SQL scripts to the `data/sql_scripts/duckdb` directory.
5. If you want to test the end-to-end evaluation time, you need to provide the SQL scripts to create the tables and insert the data into the tables for `PostgreSQL` as well. Add the SQL scripts to the `data/sql_scripts/postgresql` directory.
6. Add the new benchmark to `src/lpbound/config/paths.py` so that the code can find the data and the SQL scripts. You only need to add the name of the benchmark to the `CSV_DATA_DIR_MAP` and `WORKLOAD_TO_DB_MAP` dictionaries.
7. You can now run the experiments using the scripts in the `benchmarks/experiments` directory.

## Setup

Our code requires the following software to be installed:
- Python 3.11+
- `pyenv` (recommended for managing Python versions)
- `poetry` (for managing Python dependencies)
- `g++` (version 12.2.0)
- `cmake` (version 3.25.1)
- `PostgreSQL` (version 13.14)
- `duckdb` (version 0.10.1)

### Setup Datasets

We use three datasets in our experiments: DBLP, STATS and IMDB. The first two datasets are available in the `data/datasets` directory. The IMDB dataset can be downloaded via the following command (which is in `data/download_imdb_data.sh`):

```bash
cd data/datasets
wget https://event.cwi.nl/da/job/imdb.tgz
mkdir imdb
mv imdb.tgz imdb/
tar zxvf imdb/imdb.tgz -C imdb
rm imdb/imdb.tgz
```

Our code expects certain file names in lowercase (e.g., `⁠postlinks.csv`). To ensure compatibility without renaming the original files, please create the following symbolic links:
```bash
cd data/datasets/stats/
ln -s postLinks.csv postlinks.csv
ln -s postHistory.csv posthistory.csv
```


### Setup Python Environment

We use Python 3.11 for our experiments. You can use `pyenv` to install Python 3.11. First, install `pyenv` and then install Python 3.11:

```bash
pyenv install 3.11
pyenv global 3.11 # set the global Python version to 3.11
```

We use `poetry` to manage the dependencies of the project. You can install `poetry` using the following command:

```bash
pip install poetry
```

Then, install the dependencies using the following command:

```bash
poetry install 
```

### Setup C++ Accelerated Version

#### Install HiGHS Solver

Build the HiGHS solver from source. Make sure you compile the program in the directory `LpBound/src/lpbound/cpp_solver`.

```bash
git clone https://github.com/ERGO-Code/HiGHS.git
cd HiGHS
cmake -S . -B build
cmake --build build
```

#### Install C++ json library

We use a minimalistic json library for C++ to handle the statistics and constraints of the LPs for the parallel implementation of LpBound. 
To integrate the [library](https://github.com/nlohmann/json), only a single header file `json.hpp`is required.
No dependencies or built system are needed.

Execute the following in the directory `LpBound/src/lpbound/cpp_solver`

```bash
git clone https://github.com/nlohmann/json.git
```

#### Compile LpBoundC++ and set up estimation time experiments

Navigate to `LpBound/src/lpbound/cpp_solver` and execute:

```bash
bash compile.sh
```

Now, set up the parallel implementation that we use for runtime experiments. Execute the following command in this directory `LpBound/src/lpbound/cpp_solver/lpbound_parallel`:

```bash
# unzip and preprocess the input files:
unzip raw_input.zip -d raw_input
python create_input_files_subqueries.py
bash compile.sh
```


#### Compile FlowBound

Navigate to `LpBound/src/lpbound/LpFlow` and execute:

```bash
poetry run python setup.py build_ext --inplace
```

### Setup PostgreSQL 

For the evaluation time experiments, we use `PostgreSQL` version 13.14 to evaluate the runtime of the queries when `PostgreSQL` is injected with the estimated cardinalities of all subqueries of the queries.  To replicate the experiments, you need to install `PostgreSQL`, the `pg_hint_plan` extension and create the databases from the datasets.

#### Install PostgreSQL and update configuration

Install `PostgreSQL` on your machine. We used `PostgreSQL` version 13.14. To install `PostgreSQL` on Ubuntu, use the following commands:

```bash
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
echo "deb http://apt.postgresql.org/pub/repos/apt/ $(lsb_release -cs)-pgdg main" | sudo tee /etc/apt/sources.list.d/pgdg.list

sudo apt update
sudo apt install postgresql-13
sudo apt install postgresql-server-dev-13
sudo systemctl start postgresql
psql --version
```

You can create a new user for `PostgreSQL` with your own username.

```bash
sudo -u postgres createuser -s <your_username>
```

We modified the `postgresql.conf` file to set the shared memory to `4GB`, worker memory to `2GB`, OS cache size to `32GB`, and max parallel workers to `6`. See the `data/postgresql.conf` file for the modified values.


#### Install pg_hint_plan extension

We need the `pg_hint_plan` extension for `PostgreSQL` to inject the estimated cardinalities. To install the extension:

```bash
git clone https://github.com/ossc-db/pg_hint_plan.git
cd pg_hint_plan
git checkout PG13 # for PostgreSQL 13
sudo apt-get install make
sudo make
sudo make install
```

#### Create databases and import data

The scripts to create the databases from the datasets are in the datasets directory. To create the databases, run the following commands:

```bash
cd data
bash psql_create_job_benchmark.sh  # for job* benchmarks
bash psql_create_stats_benchmark.sh  # for stats benchmark
```



## Running Experiments

The scripts for the experiments are in the `benchmarks/experiments` directory. The experiment results that we present in the paper are stored in the `results` directory. Re-running the experiments (with different parameters) will overwrite the results.  The jupyter notebooks used to generate the plots are stored in the `notebooks` directory.

We explain how to reproduce the experiments in **Section 6 Experimental Evaluation** of the paper.

### Section 6.2: Estimation Errors (Figures 5, 6, and 7)

**Figure 5 & 7**: Estimation errors for acyclic and group-by queries in the `job-join`, `job-range`, `job-light`, and `stats` benchmarks

To run the estimation experiments for both acyclic and group-by queries, use the following commands:

```bash
# For acyclic queries (Figure 5 in the paper)
poetry run python experiments/accuracy_acyclic.py

# For group-by queries (Figure 7 in the paper)
poetry run python experiments/accuracy_groupby.py
```

The scripts create the statistics for the queries in the acyclic and groupby benchmarks and estimate the cardinalities of the queries in `workloads`. The results are stored in `results/accuracy_acyclic` or `results/accuracy_groupby`.
We describe briefly how the scripts work:

#### Data Preparation
1. Our program uses `DuckDB 0.10.1` to generate the statistics for the benchmark. When the script is run for the first time, it will create a DuckDB database at `data/duckdb/{DB}_duckdb.db` and import the data using the SQL scripts in the `data/sql_script/duckdb` directory. 

#### Generating Statistics
2. Once the database is created, the script reads the schema of the database stored in `benchmarks/schemas`. These files specify the schema of the tables in the database and which columns are join columns, selection columns, or group-by columns.
3. The default configuration is stored in `src/lpbound/config/lpbound_config.py`, which specifies the parameters for the statistics generation such as the lp-norms to compute the number of MCVs to use, the number of histogram buckets, etc.
4. Following the configuration, the script generates the SQLs for computing the statistics for the queries in the benchmark. These SQLs are stored in `output/statistics_sql`. 
5. Then, the script uses DuckDB to compute the statistics for the queries in the benchmark. The statistics are stored in the table `norms` in the DuckDB database.

#### Estimating Cardinalities
6. The script reads and parses the queries in the `benchmarks/workloads/`, and generates the linear programs for the queries. The linear programs are dumped in `output/lp_programs`.
7. The script uses the lp solver to estimate the cardinalities of the queries. The results are stored in `results/accuracy_acyclic` or `results/accuracy_groupby`.

Figure 5 (acyclic queries) and Figure 7 (group-by queries) in the paper are generated from the results of these experiments using the notebooks `accuracy_acyclic.ipynb` and `accuracy_groupby.ipynb` in the `notebooks` directory.

**Figure 6**: Estimation for cyclic queries in the Subgraph Matching benchmark

For these experiments, LP_flow is required. Ensure you have installed the necessary dependencies as outlined in Section *Setup*. Run the following command to generate the results for the cyclic queries:

```bash
poetry run python experiments/accuracy_cyclic.py
```

The script generates the statistics for the cyclic queries and estimates the cardinalities of the queries in `workloads` using LP_flow. The results are stored in `results/accuracy_cyclic`.

Figure 6 in the paper is generated from the results of this experiment using the notebook `accuracy_cyclic.ipynb` in the `notebooks` directory.


### Section 6.3: Estimation Times (Table 2)

We evaluate the time taken by LpBound to estimate the cardinalities of the queries.  To run the estimation time experiments, use the following command:

```bash
poetry run python experiments/estimation_time.py
```

The script reads the pre-generated lp-programs for the queries stored in `src/lpbound/cpp_solver/lpbound_parallel`, initializes the solver, constructs the LP instances, and estimates the cardinalities of the queries. The results are stored in `results/estimation_time`. The estimation time is measured in two ways:
1. The cardinality of all subqueries for a benchmark are computed sequentially and the estimation time per subquery is reported
2. The cardinality of all subqueries of a benchmark query are computed in parallel and the estimation time for all subqueries of a query is reported

The results of the experiments are in Table 2 in the paper. The notebook `estimation_time.ipynb` in the `notebooks` directory is used to generate the data for Table 2.
Note that the estimation times can be slightly faster than reported in the paper due to a recent memory management optimization.

### Section 6.4: Space Requirements (Table 3)

We evaluate the space requirements of LpBound by measuring the number of lp-norms stored in the database.  To run the space requirements experiments, use the following command:

```bash
poetry run python benchmarks/experiments/space_usage.py
```

The results are stored in `results/space_usage`.

### Section 6.5: Time to Compute the Statistics (Table 4)

We evaluate the time taken to compute the statistics for the queries in the benchmarks.  To run the time to compute the statistics experiments, use the following command:

```bash
poetry run python benchmarks/experiments/statistics_computation_time.py
```

The results are stored in `results/statistics_computation_time`.

### Section 6.6: From Cardinality Estimation to Query Plans (Figure 8)

This experiment evaluates the end-to-end evaluation time of the queries when the estimated cardinalities of the subqueries are used.  

To run the experiments, you need to create the databases for `PostgreSQL` as described in the [Setup](#setup) section. You might need to change the `benchmarks/experiments/utils/postgres_config.py` and `benchmarks/experiments/utils/postgres_utils.py` to match your `PostgreSQL` server, e.g., the port number.

Once you have created the databases, you can run the experiments using the following command:

```bash
poetry run python benchmarks/experiments/evaluation_time.py
```

The script performs the following steps:

1. Estimate the cardinalities of all connected subqueries in the `acyclic` benchmark and store the results in `results/evaluation_time/subquery_estimations/{benchmark}`.
2. Evaluate the queries in the `acyclic` benchmark using `PostgreSQL` with the estimated cardinalities injected using the `pg_hint_plan` extension. The evaluation time is stored in `results/evaluation_time`. 
3. Note that evaluating all queries might take a few hours to complete, where the majority of the time is spent on a few `job-range` queries. The `job-light` benchmark should take less than 10 minutes and the `stats` benchmark should take around 30 minutes.

Figure 8 in the paper is generated from the results of this experiment using the notebook `evaluation_time.ipynb` in the `notebooks` directory.

### Section 6.7: Performance Analysis

The experiments for **Figures 9-11**, which analyze the performance impact of various parameters (e.g., number of MCVs, number of norms).

For Figure 9, you need to run the following command:

```bash
poetry run python benchmarks/experiments/num_norms_effectiveness.py
```
The results will be stored in `results/num_norms_effectiveness`, and you can generate the figure using the notebook `num_norms_effectiveness.ipynb` in the `notebooks` directory.

For Figure 10, you need to run the following command:
```bash
poetry run python benchmarks/experiments/mcvs_effectiveness.py
```
The results will be stored in `results/mcvs_effectiveness`, and you can generate the figure using the notebook `mcvs_effectiveness.ipynb` in the `notebooks` directory.

#### Optimizations for LpBound’s LPs: Figure 11 - LpBase vs. LpFlow vs. LpBerge

To reproduce the experiments whose results are presented in Fig. 11, follow these instructions:

```
# Make bash scripts executable
chmod +x src/lpbound/cpp_solver/lpbound_parallel/run_lpberge.sh src/lpbound/cpp_solver/lpbound_parallel/run_lpbase.sh 

# run experiments:
poetry run python python benchmarks/experiments/lpbase_lpflow_lpberge_comparison.py
```

The results will be stored in `results/estimation_time`, and you can generate the figure using the notebook `runtime_by_method.ipynb` in the `notebooks` directory.

## Citation

If you use this code in your research, please cite our [paper](https://dl.acm.org/doi/10.1145/3725321).
