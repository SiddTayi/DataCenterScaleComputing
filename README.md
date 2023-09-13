# DataCenterScaleComputing
CSCI 5253

## LAB 1

- Create a docker image using the command
`docker run -it test:pandas`

- Open VS code (preferably), and create a pipeline.py and docker files.
`code .`

- Fetching data from my git repo: DataMining.
  `https://raw.githubusercontent.com/SiddTayi/CSCI-5502-DataMining/main/Assignments/Assignment%206/Power.csv`
      
- Update the docker file with the info:
    - The python version
    - Pandas installation
    - COPY pipeline.py file to the container
    - Execute the file while running the container

- Build the pipeline.py file using the command:
`docker run -it test:pandas `https://raw.githubusercontent.com/SiddTayi/CSCI-5502-DataMining/main/Assignments/Assignment%206/Power.csv` 'target.csv'`

- Mount the file to docker volume to check the file contents. 
