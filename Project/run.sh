pip install configparser
echo pip install configparser
echo sudo yum install python-devel postgresql-devel
sudo yum install python-devel postgresql-devel -y
echo pip install psycopg2
pip install psycopg2
echo spark-submit --master yarn ./etl.py
#spark-submit --master yarn ./etl.py