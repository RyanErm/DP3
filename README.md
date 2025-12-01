# DP3

## File Structure
This repository contains the following

- Scripts
  - producer.py – Produces data from WMATA's APIs to Kafka
  - consumer.py – Consumes data from Kafka
  - analysis.py – Creates visualizations

- Plots
  - Heatmap Plot – Heatmap of locations of buses
  - Speed vs. Delay Plot – The speed and delay of buses
  - Route Comparison Plot – Comparison of average delay between two routes

- .gitignore – Files to be ignored
- docker-compose.yml – Docker setup file
- requirements.txt – Required packages
- README.md – Description

## Overview
This repository contains the code to gather data two API's from the Washington Metropolitan Area Transit Authority (WMATA). The two API's focus on updates and positions of various busses. After the data is collected, different visualizations can be made. Please follow the instructions below to run the code. 

## Execution
First, ensure that the proper packages are install on your computer or virtual environment via the requirements.txt file. 

Ensure that Docker is up and running on your computer. Then run the following command:

```bash
docker compose up -d
```
Once that has executed successfully, you can collect data from both Metro API's and produce to Kafka. However, first change directories with this command:

```bash
cd scripts
```
Then you can run the following command to gather the data:

```bash
python producer.py
```
The amount of time for this script is variable.

Then to consume the update data from Kafka, run this command:

```bash 
python consumer_updates.py
```

To consume all the positions data from Kafka, run this command:

```bash
python consumer_positions.py
```

Finally to generate plots run the following command:

```bash
python analysis.py
```
Please note! This step will ask you to input two different bus lines that you would like to compare (e.g. "C41"). Then a plot will be generated based on those two options.

## Additional Details
The updates API will give 1000-2000 data points per call. The positions API will give 300-500 data points per call. Allowing the producer script to run for 30-40 minutes should allow the program to collect at least 50,000 data points.  