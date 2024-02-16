# Assignement1
YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit
Introduction

YouTube Data Harvesting and Warehousing is a project that aims to allow users to access and analyze data from multiple YouTube channels. The project utilizes SQL, MongoDB, and Streamlit to create application that allows users to retrieve, store, and query YouTube channel and video data.

**Project Overview**

The YouTube Data Harvesting and Warehousing project consists of the following components:

Streamlit Application: A user-friendly app built using Streamlit library, allowing users to interact with the application and perform data retrieval and analysis.
YouTube API Integration: Integration with the YouTube API to fetch channel and video data based on the provided channel ID.
MongoDB Data : Storage of the retrieved data in a MongoDB database, providing a flexible and scalable solution for storing unstructured and semi-structured data.
SQL Data Warehouse: Migration of data from the data lake to a SQL database, allowing for efficient querying and analysis using SQL queries.
Data Visualization: Presentation of retrieved data using Streamlit's data visualization features, enabling users to analyze the data.

**Install and Setup**

1. Tools Install
VS Code
Python 3.11.0 or higher.
MySQL.
MongoDB.
Youtube API key.

2. Requirement Libraries to Install
pip install google-api-python-client, pymongo, mysql-connector-python,pandas, numpy, streamlit.

3. Import Libraries
Youtube API libraries
import googleapiclient.discovery
from googleapiclient.discovery import build

File handling libraries
import json
import re

MongoDB
import pymongo

SQL libraries
import mysql.connector

pandas, numpy
import pandas as pd
import numpy as np

Dashboard libraries
import streamlit as st

**E T L Process**

a) Extract data
Extract the particular youtube channel data by using the youtube channel id, with the help of the youtube API developer console.

b) Load data
Data is stored in the MongoDB database, also It has the option to migrate the data to MySQL database from the MongoDB database.



