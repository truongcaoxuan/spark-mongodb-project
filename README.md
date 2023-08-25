# Setup Environment in Windows 10

## I. Pre-requirement

Download Java Development Kit (JDK)

* Download Oracle’s JDK (commercial) – *you can use this in development and testing for free, but if you use it in production you have to pay for it*: <https://www.oracle.com/in/java/technologies/downloads/>
* Download Oracle’s OpenJDK (open source) – *you can use this for free in any environment*: <https://jdk.java.net/>

Download Apache Spark / Apache Kafka

* Download Apache Spark™ - *a multi-language engine for executing data engineering, data science, and machine learning on single-node machines or clusters*: <https://spark.apache.org/downloads.html>
* Download Apache Kafka - *an open-source distributed event streaming platform used by thousands of companies for high-performance data pipelines, streaming analytics, data integration, and mission-critical applications*: <https://kafka.apache.org/downloads>

Download Windows binaries for Hadoop versions (Winutils)

* Winutils repo link: <https://github.com/steveloughran/winutils>
* Winutils hadoop-2.7.1: <https://raw.githubusercontent.com/steveloughran/winutils/master/hadoop-2.7.1/bin/winutils.exe>
* Winutils hadoop-2.8.1: <https://raw.githubusercontent.com/steveloughran/winutils/master/hadoop-2.8.1/winutils.exe>
* Winutils hadoop-3.0.0: <https://raw.githubusercontent.com/steveloughran/winutils/master/hadoop-3.0.0/bin/winutils.exe>

Note

* *Extract `java` file into `C` drive*
* *Extract `spark` file into `C` drive*
* *Extract `kafka` file into `C` drive*
* *Extract `winutils.exe` file into `C:\hadoop\bin` folder*

## II. Setup system environment variables

Example

| ENVIRONMENT VARIABLE NAME | VALUE                      |
| ------------------------- |:---------------------------|
| KAFKA_HOME                | C:\kafka_2.12-3.3.2        |
| SPARK_HOME                | C:\spark-3.3.2-bin-hadoop3 |
| HADOOP_HOME               | C:\hadoop                  |
| JAVA_HOME                 | C:\Java\jdk-10.0.2         |

Edit **`Path`** variable from environment variable and add new values below

```path
%KAFKA_HOME%\bin\windows
```

```path
%SPARK_HOME%\bin
```

```path
%HADOOP_HOME%\bin
```

```path
%JAVA_HOME%\bin
```

## III. Setup Anaconda environment

Download Anaconda - *Conda is an open-source package and environment management system*

* Anaconda link download: <https://www.anaconda.com/download>

Note

* *Install `anaconda.exe` file*

1. Open **`Anaconda Prompt`** and execute command below

Create new environment for project

```conda
conda create -n pyspark-env python=3.9 -y
conda activate pyspark-env
```

Using **`conda`** to install openjdk, findspark

```conda
conda install openjdk
conda install -c conda-forge findspark
```

Note

* openjdk-11.0.13

2. Using **`pip`** to install all necessary python library specified in requirements.txt file using below command.

```pip
pip install -r requirements.txt
```

3. Install Jupyter notebook & run PySpark

Open Anaconda Navigator

* If you don’t have Jupyter notebook installed on Anaconda, just install it by selecting `Install` option.
* Post-install, Open Jupyter by selecting `Launch` button.

In order to run PySpark in Jupyter notebook first, you need to find the PySpark Install.
If you get pyspark error in jupyter then then run the following commands in the notebook cell to find the PySpark.

```findspark
import findspark
findspark.init()
findspark.find()
```

## IV. Git command

Command to upload your code to gihub repo

```git
git init
git add .
git commit -m "first commit"
git branch -M main
git remote add origin <github_repo_link>
git push -u origin main
```
