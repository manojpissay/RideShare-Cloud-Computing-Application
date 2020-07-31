# Cloud Computing Project

For orchestrator VM :
--Navigate to Project/orchestrator/ folder and run(with sudo access):
  1)  docker build . -t theslave:latest
  2)  docker-compose up
--In the orchestrator.py file check for the network name.
--This will get the orchestrator up and running along with zookeeper rabbitmq and 2 workers.

For Users and Rides VM :
--Navigate to Project/users or Project/rides and (folder with docker-compose.yml file) run (assumming sudo access):
  docker-compose up 
--This will get the user and ride container running in their repective VMs.
  
