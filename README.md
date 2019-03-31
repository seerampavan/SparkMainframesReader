# Read the EBCIDC file using copybook and save it to HDFS

## build the project
    mvn clean install
## run the code using below command 
    spark-submit --class com.mainframe.CopyEBCIDCToHDFS \
    --master yarn --deploy-mode client /cluster--executor-memory 512MB --num-executors 2 --executor-cores 1 \
    MainframeReaderExecute.jar \
    clusterORLocal=l copybookHDFSInputPath=<HDFS PATH>/example.cbl \
    dataFileInputPath=<HDFS PATH>/data/ outputPath=<HDFS PATH> isOverwriteDataOk=yes
    
    example: 
    spark-submit --class com.mainframe.CopyEBCIDCToHDFS \
    --master yarn --deploy-mode client --executor-memory 512MB --num-executors 2 --executor-cores 1 \
    MainframeReaderExecute.jar \
    clusterORLocal=l copybookHDFSInputPath=hdfs://localhost:9000/data1/test1/cpbook/example.cbl \
    dataFileInputPath=hdfs://localhost:9000/data1/test1/data/ outputPath=test3/dataout/ isOverwriteDataOk=yes
    
