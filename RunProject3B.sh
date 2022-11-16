for (( i = 0; i < 150; i++ )); do
	make project3b 2>&1 > log.txt
	if ! grep -n FAIL log.txt ; then
		rm log.txt
		echo $(i) PASS
	else
		cp log.txt "$(date)_FAIL.log"
		echo $(i) FAIL
	fi
done
