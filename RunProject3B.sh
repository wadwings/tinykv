for (( i = 0; i < 150; i++ )); do
	make project3b > log.txt 2>&1
	if ! grep -n FAIL log.txt ; then
		rm log.txt
		echo "$(i) PASS"
	else
		cp log.txt "$(date)_FAIL.log"
		echo "$(i) FAIL"
	fi
done
