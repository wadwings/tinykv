n=1

while [ $n -le 10 ]
do
  echo "This is the $n times run project2b"
  make project2b >> project2b.log 2>&1
  n=$((n + 1))
done