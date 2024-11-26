# Airflow Weather Data Pipeline

## Overview

This project leverages Apache Airflow to create an end-to-end workflow for retrieving, cleaning, and analyzing weather data. 
The pipeline automates the process of fetching data from a weather API, processing and storing the cleaned data, and training 
different machine learning models on the dataset. The ML models are evaluated in order to select the best performing one.

## Features
- **Data Retrieval**: Collects raw weather data from an external API.
- **Data Cleaning**: Cleans and formats the raw data to ensure consistency and usability.
- **Data Storage**: Saves the cleaned data to a local or remote storage system for later use.
- **Machine Learning**: Trains a machine learning algorithm using the processed dataset.

## Prerequisites
Before running the project, ensure that:
1. The requirements in requirements.txt are installed.
2. You have a valid API key to access the OpenWeatherMap API.

  
