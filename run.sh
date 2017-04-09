JAR=analysis.jar
CLASS=cs455.hadoop.Q5.Q5Job
IN=/data/census-tiny
OUT=/out/Q2-out

hadoop fs -rmr ${OUT}
hadoop jar dist/${JAR} ${CLASS} ${IN} ${OUT}

rm -r ~/CS455/CS455-HW3/out/*
hadoop fs -get ${OUT}/* ~/CS455/CS455-HW3/out/
