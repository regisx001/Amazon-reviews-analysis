sudo chown -R 1000:1000 ./data   # if spark user maps to 1000 on host
# OR more simply:
sudo chmod -R 777 ./data




# AIRFLOW PERMISSIONS

# Give the airflow user (uid 50000) ownership of the logs folder
sudo chown -R 50000:50000 ./airflow/logs
sudo chmod -R 775 ./airflow/logs