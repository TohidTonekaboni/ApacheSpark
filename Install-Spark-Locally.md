## Install Spark Locally

#### Install Java 

- check out java is installed in `WSL`

```bash
$ wsl
$ sudo apt install openjdk-8-jdk
$ sudo update-alternatives --config java

```



#### Download & Prepare the Local  Environment

- got to [Downloads | Apache Spark](http://spark.apache.org/downloads.html) and download the latest spark archive file (save it in `section2/spark` folder)

![](images\spark-download.png)

- got to `spark` folder in command line (right click & select `open Powershell here`)

- type `wsl` or go into Linux Terminal

  ```bash
  $ mkdir spark
  $ tar xzvf spark-3.5.0-bin-hadoop3.tgz
  $ cp -r spark-3.5.0-bin-hadoop3/* spark/
  $ rm -r spark-3.5.0-bin-hadoop3
  
  ```

  

  ## Start Master & Worker

  we want to setup a `Standalone Spark Cluster` ([See the Docs](https://spark.apache.org/docs/latest/spark-standalone.html))

  ##### Follow this steps :

- Start the `master node`

  ```bash
  $ sudo ./spark/sbin/start-master.sh --webui-port 8080
  starting org.apache.spark.deploy.master.Master, logging to ....out
  ```

  - open Spark Web UI : http://localhost:8080

  ![](images\Saprk-Ui.png)

- Start the first  `Worker node `

  ```bash
  $ sudo ./spark/sbin/start-worker.sh  spark://Mojtaba-PC.:7077 --webui-port 8081 -p 9911  -c 1 -m 512M
  ```

  - check out the web UI again and the newly created worker must be showed up.

  ![](images\worker-showed-up.png)

- Stop it and check the Spark Web UI again (`stop-worker.sh`)

  ### Start 2 Workers in the Standalone Mode

  ```bash
  $ cp spark/conf/spark-env.sh.template spark/conf/spark-env.sh
  $ nano spark/conf/spark-env.sh
  ```
  add this lines :
  ```bash
  SPARK_WORKER_CORES=2
  SPARK_WORKER_INSTANCES=2
  SPARK_WORKER_MEMORY=512M
  ```
  or you can set `SPARK_WORKER_MEMORY=1G`
  ```bash
$ sudo chmod +x spark/conf/spark-env.sh
$ sudo ./spark/sbin/start-worker.sh  spark://Mojtaba-PC.:7077
  ```

  

  ## Start Spark Shell

- Scala Shell : 

  ```bash
  $ sudo ./spark/bin/spark-shell
  
  Ctrl+D for exit
  
  ```

- Python Shell (pyspark)

```bash
  $ export SPARK_HOME=$(pwd)/spark
  $ export PATH=$SPARK_HOME/bin:$PATH
  $ export PATH=$SPARK_HOME/sbin:$PATH
  $ source ~/.bashrc
  $ sudo ./spark/bin/pyspark
  Python 3.8.5 (default, Jan 27 2021, 15:41:15)
  [GCC 9.3.0] on linux
  ...
  Welcome to
        ____              __
       / __/__  ___ _____/ /__
      _\ \/ _ \/ _ `/ __/  '_/
     /__ / .__/\_,_/_/ /_/\_\   version 3.1.2
        /_/
  
  Using Python version 3.8.5 (default, Jan 27 2021 15:41:15)
  Spark context Web UI available at http://172.17.200.170:4040
  Spark context available as 'sc' (master = local[*], app id = local-1629367902764).
  SparkSession available as 'spark'.
  >>>data=sc.parallelize(range(0,100))
  >>> data.filter(lambda x: x%3==0).collect()
  [0, 3, 6, 9, 12, 15, 18, 21, 24, 27, 30, 33, 36, 39, 42, 45, 48, 51, 54, 57, 60, 63, 66, 69, 72, 75, 78, 81, 84, 87, 90, 93, 96, 99]
```
- checkout the Web UI : http://172.17.200.170:4040  
- `PpySpark` runs a local cluster independent of our running cluster



![](images\pyspark1.png)

![](images\pyspark2.png)

![](images\pyspark3.png)



```bash
>>>exit()
```



#### Start Jupyter Notebook

- set `Environment Variables` :

  ```bash
  
  $ export PYSPARK_DRIVER_PYTHON=jupyter
  $ export PYSPARK_DRIVER_PYTHON_OPTS='notebook'
  ```

  

- install `findspark` in order to python to easily find the `spark cluster`

  ```bash
  $ python -m venv .venv 
  $ source ./.venv/bin/activate
  $ pip3 install jupyter pyspark jupyter-core findspark
  ```

- start `Jupyter Notebook`

  ```bash
  $ cd notebooks
  $ jupyter notebook
  [I 15:06:16.286 NotebookApp] Serving notebooks from local directory: /mnt/c/Users/Administrator/Desktop/17/spark/notebooks
  [I 15:06:16.286 NotebookApp] The Jupyter Notebook is running at:
  [I 15:06:16.286 NotebookApp] http://localhost:8888/?token=10953699179b25fab86982f353a561cc237d7bf96f2ff9c5
  [I 15:06:16.286 NotebookApp]  or http://127.0.0.1:8888/?token=10953699179b25fab86982f353a561cc237d7bf96f2ff9c5
  [I 15:06:16.286 NotebookApp] Use Control-C to stop this server and shut down all kernels (twice to skip confirmation).
  [C 15:06:16.543 NotebookApp]
  
      To access the notebook, open this file in a browser:
          file:///home/mojtaba/.local/share/jupyter/runtime/nbserver-6843-open.html
      Or copy and paste one of these URLs:
          http://localhost:8888/?token=10953699179b25fab86982f353a561cc237d7bf96f2ff9c5
       or http://127.0.0.1:8888/?token=10953699179b25fab86982f353a561cc237d7bf96f2ff9c5
       
  ```

  - click on the link provided above.

![](images\notebook.png)

#### Stop Master/Workers

- in spark folder, run 

  ```bash 
  $  ./spark/sbin/stop-master.sh
  ```

  

- in spark folder, run   :

  ```bash
  $ ./spark/sbin/stop-worker.sh
  ```

  - but because Spark Variables is set to spark folder, we need to run these commands to be sure every worker is down :

  ```bash
  $  for pid in $(jps | grep Worker | awk '{print $1}'); do 
  kill -9 $pid;
  done
  ```

  
