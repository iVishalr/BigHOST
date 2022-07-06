# Big Data Evaluator Backend

This repository contains the source code, solution and scripts necessary to run the backend server and evaluate submissions. 

# Setup the Evaluator

1. Clone the repository
```sh
git clone https://github.com/aditeyabaral/bd-evaluator
```

2. Install dependencies

```sh
sudo apt install python3-rq redis-server -y;
```

3. Setup MongoDB

```sh
sudo apt install curl -y

curl -fsSL https://www.mongodb.org/static/pgp/server-4.4.asc | sudo apt-key add -
echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/4.4 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-4.4.list

sudo apt update -y
sudo apt install mongodb-org -y

sudo systemctl start mongod.service
```

4. Install `python3` dependencies

It is preferred that all `python3` dependencies are **installed into the system environment and not a virtual environment**. If a virtual environment is used, the virtual environment needs to be activated for **all processes** that are to be run while executing the backend server.

```sh
pip3 install -r requirements.txt
```

# Run the Evaluator

All these commands need to be executed from the **same directory**, preferably from `src/`

1. Run `redis-server`

```sh
redis-server
```

2. Run `rq`

```sh
rq worker
```

3. Ensure that `mongodb` is running
```sh
sudo systemctl start mongod
```

4. Run the server
```sh
python3 main.py
```

# Fixes to Frequent Issues

## `rq` trying to parse empty string as `datetime`

This occurs due to a bug on in the `rq` library. Head over to the directory where `rq` is installed and navigate to the `utils.py` file. Look for this function:

```py
def str_to_date(date_str):
    if date_str is None:
        return
    else:
        return utcparse(as_text(date_str))
```

Replace the `if` condition with ```if not date_str``` such that the function now looks like this:

```py
def str_to_date(date_str):
    if not date_str:
        return
    else:
        return utcparse(as_text(date_str))
```

## `evaluation has no attribute evaluation`

This occurs when you have not run all processes from the same directory and/or you have not activated the virtual environment for all processes. 

To fix this, ensure that the virtual environment is activated on all processes (or preferably do away with the virtual environment and install everything onto the system `python3` environment) and ensure that `redis-server`, `rq worker` and `python3 main.py` are all run from the same directory.

## `rq worker` says port not available

This occurs when there is already an unclosed instance of `redis-server` running on the same port. To fix this, perform the following steps.

- Find the process IDs of the running `redis-server` process
    ```sh
    ps aux | grep redis
    ```
- Kill all the redis-server processes
    ```sh
    sudo kill -9 <PID>
    ```
- Stop `redis-server` manually
    ```sh
    sudo service redis-server stop
    ```

## `mongodb` returns a Connection Timed Out Error

This occurs when MongoDB has not been started. Run the process using
```sh
sudo systemctl start mongod
```