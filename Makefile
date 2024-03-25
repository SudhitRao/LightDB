compile:
	mvn clean compile assembly:single

exec:
	java -jar target/lightdb-1.0.0-jar-with-dependencies.jar samples/db samples/input/query1.sql samples/output/query1.csv

exec2:
	java -jar target/lightdb-1.0.0-jar-with-dependencies.jar samples/db samples/input/query2.sql samples/output/query2.csv

exec3:
	java -jar target/lightdb-1.0.0-jar-with-dependencies.jar samples/db samples/input/query3.sql samples/output/query3.csv

exec4:
	java -jar target/lightdb-1.0.0-jar-with-dependencies.jar samples/db samples/input/query4.sql samples/output/query4.csv

exec5:
	java -jar target/lightdb-1.0.0-jar-with-dependencies.jar samples/db samples/input/query5.sql samples/output/query5.csv

exec6:
	java -jar target/lightdb-1.0.0-jar-with-dependencies.jar samples/db samples/input/query6.sql samples/output/query6.csv

exec7:
	java -jar target/lightdb-1.0.0-jar-with-dependencies.jar samples/db samples/input/query7.sql samples/output/query7.csv

exec8:
	java -jar target/lightdb-1.0.0-jar-with-dependencies.jar samples/db samples/input/query8.sql samples/output/query8.csv

exec9:
	java -jar target/lightdb-1.0.0-jar-with-dependencies.jar samples/db samples/input/query9.sql samples/output/query9.csv

exec10:
	java -jar target/lightdb-1.0.0-jar-with-dependencies.jar samples/db samples/input/query10.sql samples/output/query10.csv

exec11:
	java -jar target/lightdb-1.0.0-jar-with-dependencies.jar samples/db samples/input/query11.sql samples/output/query11.csv

exec12:
	java -jar target/lightdb-1.0.0-jar-with-dependencies.jar samples/db samples/input/query12.sql samples/output/query12.csv