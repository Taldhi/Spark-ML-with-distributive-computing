# You can check your ip address using 
ip addr show


# Start the master node using the following commands in your linux system
cd $SPARK_HOME/conf
cp spark-env.sh.template spark-env.sh
export SPARK_MASTER_HOST=<give your own ip address>
export SPARK_MASTER_PORT=<give your port number>
export SPARK_WORKER_CORES=<number of cores you want to provide for calculation>
export SPARK_WORKER_MEMORY=<amount of storage you want to provide , eg-4g= 4 GB>
export SPARK_WORKER_INSTANCES=<number of workers you want to connect to your master node>
$SPARK_HOME/sbin/start-master.sh # This command starts your master node



# Start your worker nodes with the following commands in your linux system
cd $SPARK_HOME/conf
cp spark-env.sh.template spark-env.sh
export SPARK_WORKER_CORES=<number of cores you want to provide for calculation>
export SPARK_WORKER_MEMORY=<amount of storage you want to provide , eg-4g= 4 GB>
$SPARK_HOME/sbin/start-worker.sh spark://<master node ip>:<master node port>



# Use this to check you master node availability and connected workers nodes in your browser
http://<master node ip>:<port>


# Now you need to make a standalone .py file and submit that file to the spark cluster to run that application 
spark-submit <your .py file name>.py  # if you are not running the file from the location of your .py file you can add the path of the file before your .py file like {spark-submit location/of/file/<your .py file name>.py}

#If after submitting the .py file you get any jupyter directory error [if you have jupyter installed in your sysytem] then you can temporariry change that path variable like following
unset PYSPARK_DRIVER_PYTHON #if you wish you can change that for permanently also 