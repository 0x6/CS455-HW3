JAR=analysis.jar
CLASS=cs455.hadoop.Q7.Q7Job
IN=/data/census
OUT=/home/census/output

hadoop fs -rmr ${OUT}
hadoop jar dist/${JAR} ${CLASS} ${IN} ${OUT}

rm -r ~/CS455/CS455-HW3/out/*
hadoop fs -get ${OUT}/* ~/CS455/CS455-HW3/out/
