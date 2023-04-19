<h1 align="center">
    <span>BigHOST</span>
</h1>

<div align="center">
[![HitCount](https://hits.dwyl.com/Cloud-Computing-Big-Data/backend-2022.svg?style=flat-square)](http://hits.dwyl.com/Cloud-Computing-Big-Data/backend-2022)
</div>

This is the official repository of "BigHOST: Automatic Assignment Grading System", CCGridW 2023.

BigHOST is an autograding system that is developed to evaluate Big Data Assignments. BigHOST employs a simple architecture that is scalable and provides a fair environment for executing multiple Big Data jobs in parallel.

## Project Setup

### Requirements

The following python packages are required for running BigHOST

1. redis
2. psutil
3. Flask
4. Flask_Cors
5. python-dotenv
6. Requests
7. pymongo
8. docker_py

The python packages can be installed by using the `requirements.txt` file provided.

The following system packages are required to run BigHOST

1. Docker
2. tmux
3. vim
4. tmuxinator

Ensure that you have a proper Docker installation on your system before running the project.

[tmuxinator](https://github.com/tmuxinator/tmuxinator) is used to launch tmux session with multiple windows simultaneously.

### Setting up environment variables

```console
MONGO_URI_RR=<mongodb_URI>
MONGO_URI_EC=<mongodb_URI>

MAIL_USER=<email_id>
MAIL_PASSWD=<password>

BACKEND_INTERNAL_IP=<ip_address>
```

Add the above environment variables to a `.env` file in project root directory.

Replace the contents of `<...>` with the appropriate values. The IP address of backend server must be `localhost` if you are running locally else `EXTERNAL_IP_ADDR` of the backend server.

### Setting up BigHOST configuration file (important)

The `config/evaluator.json` stores the configuration information for all components of BigHOST. Please refer to the documentation to get more details on each parameter in the configuration file. The configuration file must be setup based on the specifications of the system that BigHOST is running on. Incorrect configuration could lead to either underutilization of the system or loss of performance.

### Starting BigHOST

Assuming you have all the required packages, type the following commands in separate terminals. Note that you need to execute the commands in project's root directory.

#### Backend Server

```console
$ tmuxinator start project -n backend -p ./scripts/backend.yaml
```

#### Evaluation Engine

```console
$ tmuxinator start project -n evaluator -p ./scripts/evaluator.yaml
```

## Documentation

For documentation, please refer to [DOCUMENTATION.md](./doc/README.md).

## Citation

This work has been accepted at _2023 IEEE/ACM 23rd International Symposium on Cluster, Cloud and Internet Computing Workshops (CCGridW)_.

Official BibTeX citation will be updated once available.

## License

MIT
