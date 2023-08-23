#! /bin/bash
# 使用bash autograde.sh执行
files=$(ls *.sql)
declare -A scores=(
	["q1_sample.sql"]=0
	["q2_sci_fi.sql"]=5
	["q3_oldest_people.sql"]=5
	["q4_crew_appears_most.sql"]=10
	["q5_decade_ratings.sql"]=10
	["q6_cruiseing_altitude.sql"]=10
	["q7_year_of_thieves.sql"]=15
	["q8_kidman_colleagues.sql"]=15
	["q9_9th_decile_ratings.sql"]=15
	["q10_house_of_the_dragon.sql"]=15
)
res=0
for i in $files
do
	if ! [ -s $i ]
	then
		echo "skipping $i"
		continue
	fi

	echo "************************"
	echo "testing $i"
	SECONDS=0
	diff <(echo ".read placeholder/$i" | sqlite3 imdb-cmudb2022.db) <(echo ".read $i" | sqlite3 imdb-cmudb2022.db)
	return_val=$?

	echo "elapsed time $SECONDS"
	if [ $return_val -eq 0 ] 
	then
		echo "$i passed!"
		(( res = $res + ${scores[$i]} ))
	else
		echo "$i failed"
	fi
done

echo "final score $res"