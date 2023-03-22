# Big Data Backend Engine

This project is currently under development.

## Current Architecture

<p align="center" padding="100px">
    <img src="./images/detail-architecture.png" alt="detail-architecture"></img>
</p>

## Build Steps

### Instructions for deployment on GCP

RR Campus TA Team is using Singapore to Deploy. (asia-southeast1)

Also you will need to request additional quotas in GCP. By default, you are limited to 8 vCPUs on trial account.

1. Setup a VPC Network.
2. Reserve 2 Static IP addresses in VPC under your region of interest
3. Reserve 2 External IP addresses as well one for backend and one for evaluator
4. Create a VM for backend first. While creating VM make sure you name it properly and select the correct region.
5. Select Ubuntu Image and allocate as much storage as you might need.
6. Allow HTTPS and HTTP traffic
7. In networking section, make sure you attach the configured internal and external IPs to your VM.
8. If you are testing, enable SPOT instances in management tab. If you are deploying it for production, use Standard VMs itself. This will help you offset costs.
9. Create VM
10. Now create another VM for evaluator.
11. Make sure you follow all the above steps to setup the evaluator VM as well.
12. Make sure you allocate more resources to this VM as this will do all the computation for us.
13. Attach the external and internal IP addresses.
14. Create VM

### Inside the Backend VM

1. SSH into your backend VM and type the commands in `scripts/install.sh`.
2. DO NOT execute it directly assuming its a bash script. It may not work. I haven't tested that.
3. Execute each command one by one.
4. Once all the commands are executed, it's time to transfer the backend folder to the VM.
5. Now on your local machine, make sure `.env` is correctly setup reflecting the correct Internal and External IPs for both the VMs
6. Make sure that you have the pipeline working on your local machines before deploying on GCP. Instructions for the same is given below.
7. I like to use wormhole to do this. Its nice and easy.
   1. On your local machine type, `wormhole send /path/to/backend-2022`. This will zip the files and give you a token. Copy that token and paste that in the backend VM.
   2. Press 'y' to accept the incomming files.
8. Now in you backend VM, go to backend-2022, and install the python dependecies using `pip3 install -r requirements.txt`.
9. Now its time to start the backend server.
10. Execute `docker run -d --name redis-stack-server -p 6379:6379 redis/redis-stack-server:latest`
11. We will lauch everything at once using tmuxinator.
12. In the terminal type the command `tmuxinator start project backend -p ./scripts/backend.yaml` to lauch the project.
13. Go to the tmux session by attaching to it (google if you don't know).
14. Switch to the `flask_backend` window by pressing `ctrl+b` and `1`.
15. Restart the flask server.
16. Now our backend is ready for accepting submissions.

### Inside the Evaluator VM

1. SSH into your backend VM and type the commands in `scripts/install.sh`.
2. DO NOT execute it directly assuming its a bash script. It may not work. I haven't tested that.
3. Execute each command one by one.
4. Once all the commands are executed, it's time to transfer the backend folder to the VM.
5. Now on your local machine, make sure `.env` is correctly setup reflecting the correct Internal and External IPs for both the VMs
6. Make sure that you have the pipeline working on your local machines before deploying on GCP. Instructions to do the same is given below.
7. I like to use wormhole to do this. Its nice and easy.
   1. On your local machine type, `wormhole send /path/to/backend-2022`. This will zip the files and give you a token. Copy that token and paste that in the backend VM.
   2. Press 'y' to accept the incomming files.
8. Now in you backend VM, go to backend-2022, and install the python dependecies using `pip3 install -r requirements.txt`.
9. Now its time to start the evaluator server.
10. Execute `docker run -d --name redis-stack-server -p 6379:6379 redis/redis-stack-server:latest`
11. Build the hadoop container by executing the command `cd docker && docker build -t hadoop-3.2.2:0.1 -f hadoop3.dockerfile . && cd ..`
12. We will lauch everything at once using tmuxinator.
13. In the terminal type the command `tmuxinator start project evaluator -p ./scripts/evaluator.yaml` to lauch the project.
14. Now our evaluator should be ready for processing.
15. Go to the tmux session by attaching to it (google if you don't know).
16. As a sanity check make sure that you see `Spawning 1 Thread` and `No more submissions ... | Current queue length is : 0`. If you don't see this, it means you have not setup the VMs properly. The two VMs are not able to connect to each other. Hint check your IPs. VM -> VM communication happens only through Internal IPs we reserved earlier.
17. If you don't see any problems, go to the frontend website and submit submissions. The whole pipeline will automatically pick up the submission and execute the job.

Next step. You can configure the evaluator according to your needs. All the configuration options are available at the bottom of the executor.py . You can provide how much ram / container to use. The number of backends you need. The number of workers to support the backend and so on. Note that this requires the knowledge of how executor and workers interact with each other. Please try to understand those files and only then proceed.

### Stopping The Processes

Make sure you don't kill the tmux sessions directly. It will corrupt the system. Attach to each tmux session and manually stop the processes.

### Instructions for building locally

CAUTION : Don't copy blindly.

```bash
sudo apt update
sudo apt upgrade -y
sudo apt install software-properties-common
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt install python3 python3-pip magic-wormhole tmux tmuxinator gunicorn wget pylint
```

### To install docker

```bash
sudo apt install apt-transport-https ca-certificates curl software-properties-common && \
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add - && \
sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu bionic stable" && \
sudo apt-get install docker-ce docker-ce-cli containerd.io -y && \
cat <<EOF | sudo tee /etc/docker/daemon.json
{
  "exec-opts": ["native.cgroupdriver=systemd"],
  "log-driver": "json-file",
  "log-opts": {
    "max-size": "100m"
  },
  "storage-driver": "overlay2"
}
EOF&&\
sudo usermod -aG docker $USER&&\
sudo systemctl enable docker && sudo systemctl daemon-reload && sudo systemctl restart docker
```

Verify docker installation by

```bash
docker ps -a
```

To build image, type the following in terminal

```bash
cd docker
docker build -t hadoop-3.2.2:0.1 -f hadoop3.dockerfile .
cd ..
```

### Install python dependencies

```bash
pip3 install -r requirements.txt
```

### Setup .env file

Create a .env file and add the following in the file.

```bash
MONGO_URI_RR=mongodb+srv://team:bdteam-2022@bd-db-2022.plzop.mongodb.net/?retryWrites=true&w=majority
MONGO_URI_EC=mongodb+srv://bdadmin:admin123@bigdata-eccampus.8prlrhb.mongodb.net/bd?retryWrites=true&w=majority

MAIL_USER=bigadata@pes.edu
MAIL_PASSWD=bigdata_2022

BACKEND_INTERNAL_IP=<ip_address>
BACKEND_EXTERNAL_IP=<ip_address>
EVALUATOR_INTERNAL_IP=<ip_address>
EVALUATOR_EXTERNAL_IP=<ip_address>
```

ONLY FOR RR CAMPUS

Replace `<ip_address>` with the IP address of the actual server. For local development, use `localhost`.

## Running a Evaluation Simulation

Note : You should be in project root directory

### Launching using Tmuxinator

To automatically lauch the project, type the following in different terminals

#### To start up the website's backend

```bash
tmuxinator start project backend -p ./scripts/backend.yaml
```

#### To start up the evaluator

```bash
tmuxinator start project evaluator -p ./scripts/evaluator.yaml
```

#### Attaching to Tmux Sessions

To attach into the tmux sessions, type the following in different terminals

```bash
tmux attach -t backend
```

```bash
tmux attach -t evaluator
```

#### To Switch between terminals

Press `ctrl+b` and press the terminal number thats displayed on the bottom of tmux terminal

#### To detach from tmux session

Press `ctrl+b` and press `d`

## To start the project manually

#### Setting up Redis

```bash
docker stop redis-stack-server
docker rm redis-stack-server
docker run -d --name redis-stack-server -p 6379:6379 redis/redis-stack-server:latest
```

#### Job Queuer

In a new terminal type the following

```bash
gunicorn -w 2 --preload --timeout 90 --bind 0.0.0.0:10001 "job_tracker.queuer:app"
```

#### Website's Backend

In a new terminal type the following

```bash
gunicorn -w 2 --preload --timeout 90 --bind 0.0.0.0:9000 "flask_backend.app:createApp()"
```

#### Queue some submissions

To simulate queueing submissions, in a new terminal type

```bash
python3 ./client.py
```

#### Checking queue lengths through browser

Now the we have queued the submissions on website's backend, go to browser and make a GET request to the following routes

```
http://localhost:9000/queue-length
http://localhost:10001/queue-length
http://localhost:10001/output-queue-length
```

<!-- Now we will have 119 submission on website's backend and 1 submission in evaluation engine's backend. -->

#### Create directories

Create the following directories in project root directory

1. answer
2. answer/A1T1

Add the answer key to the above directory

#### Start Output Processor

In a new terminal type,

```bash
python3 -m output_processor.output
```

#### Start Mailer Service

Time to execute these submissions. In a new terminal, type

```bash
python3 -m smtp.mailer
```

#### Start Executing submissions

Time to execute these submissions. In a new terminal, type

```bash
python3 -m job_tracker.executor
```

This will start executing the jobs on docker container.

#### Clear the queues

To clear the queues, in browser make GET requests to

```
http://localhost:9000/empty-queue
http://localhost:10001/empty-queue
http://localhost:10001/empty-output-queue
```

## Ports Used

1. 10000 - Port that flask server in docker container is running. This port receives all the request for job processing
2. 9870 - HDFS Datanode WebUI
3. 8088 - Hadoop Resource Manager UI
4. 10001 - Job Queuer and Tracker Flask server
5. 9000 - Website's backend flask server
6. 6379 - Redis Docker Container
7. 19888 - JobHistoryServer

These ports can change in future.

## Backend Architecture

Let's try to keep the architecture as simple as possible while still maintaining scalability and efficiency. The backend of the entire portal will consists of two parts. One for the website and the second for our evaluation engine.

#### Website's Backend Config

1. VM with atleast 8 core cpu and 16gb ram.
2. 100 GB disk space.
3. Deployed in India to provide low latencies to students.

#### Evaluation Engine Config

1. VM with atleast 16 cores cpu and 64gb ram.
2. 200 GB disk space (disk is cheap).
3. Deployed in USA to minimize network and VM costs.

Both the Website's backend and evaluation engine will communicate with each other over network.

## Details on Web portal

<p align="center" padding="100px">
    <img src="./images/website-backend.png" alt="website-backend"></img>
</p>

The webportal will consist of a frontend and a backend. The backend will be used for serving the client requests. The backend will also communicate with the evaluation engine. The website backend will also maintain a direct connection to the MongoDB instance that's running in evaluation server.

The basic functionalities supported would be the following :

1. Students will be able to login to their respective accounts.
2. Students can change their login credentials.
3. Students can upload their scripts for evaluation.
4. Students can check the status of their submission.
5. Students can check the queue waiting time.

#### Performing initial sanity checks

When the students upload their scripts, we will perform a sanity check by compiling the scripts to check for any syntax errors and illegal modules. Only the scripts that passes this sanity check will be queued for evaluation. Scripts that do not pass the check will be terminated and an update in the database will be made to let them know their submission status. (This is where a direct connection to database from website's backend is helpful.)

The submission will be queued on the website's backend itself. This will prevent us from losing all the submissions if the evaluation engine crashes.

Instead of using RabbitMQ or Redis for queuing the submissions, I suggest we use a simple queue mechanism. One main reason I believe this idea is good is because since we are running the servers in India and US, the submissions must be transfered to US server and this network transfer will take time. So to overcome this delay we will need to prefetch submissions from India server and queue them on US server. Note that this second queue will be very small, only holding 8-16 submissions depending on the number of pods we run. While submissions are being processed in the pods, the next batch of submissions will be made ready by prefeteching it.

Implementing our own queue will also help is in finding the total wait time, finding the position of a submission in the queue and so much more.

##### How do we stream the submissions to US server?

So when the queue in US server becomes empty, the US server will send a request to the India server asking for more submissions. The India server will then create the payload of 'x' submissions and make a post request to a route on US server.

In python this can be done using the `requests` module. In javascript we should be able to use `axios` to achieve this.

## Details on Evaluation Engine

The evaluation engine is where the magic happens. We will be using Kubernetes and Docker containers to make the evaluation engine fault tolerant. This will help us prevent downtime and also helps us to process multiple submissions at once in parallel. The following figure shows the rough architecture of the backend.

<p align="center" padding="100px">
    <img src="./images/architecture.png" alt="backend-architecture"></img>
</p>

#### Job Queuing and Execution Tracker

As said earlier, we need to stream the submission files to US server where our evaluation engine will be running.

The batch of submissions sent from India will be queued here. When the pods are done with their previous batch, the batches in the queue will be passed to the pods for processing.

Execution Tracker is need to maintain a log of which submission is running on which pod. This is mainly done for extra safety. If a submission manages to crash a pod, this log will help us in determining which submission crashed the pod. Thus blacklisting will be made easier. If a submission crashes the pod, an update in MongoDB will be made. (red dashed arrow.)

#### Kubernetes Service & Load Balancer

We will be using kubernetes to orchestrate our containers. Kubernetes comes with automatic pod restarting and load balancing. This will be very useful to us to automatically restart the pods without manual intervention. Kubernetes also allows replicasets meaning we can have multiple instances of a docker container. The load balancer will evenly distribute the load to the pods. Hence we can now process submissions in parallel.

#### Pods

Each pod will have a Hadoop runtime env setup inside them. This is done by using a custome hadoop docker image. A flask server will be running inside each pod. This server will receive the submission from the above services and put the submission to execution.

##### How do we check if a submission exceeds alloted time.

To do this, the flask server will spawn a child thread alongside the main process. This thread will track the execution time of the main process. Should submission exceed the alloted time, the child process will issue a SIGTERM to kill the process groups.

An update in the database will be made to let the student know that their submission has timed out.

#### Output Queuing and Processing

Once the submission has been processed by the pod, the output files will be copied and will be sent here. We will then compare the student's output with ours and then compute the marks. The marks will then be updated in the database.

##### Why do we need to do this separately?

This needs to be separated from pods because comparision of output is relatively less computationally intensive and hence we do not want to waste pod's time in computing this. Let the pods move onto the next submission and let the output processor handle comparing outputs.

##### Why do we need another queue?

We need another queue because we have multiple pods running and each one of them can send a request to the output processor to compare the output. If all of them send at once, we need a mechanism to store the submissions somewhere and hence the queue. The size of this queue, on average, will be equal to the number of pods we are running.

#### MongoDB

To store everything :)
