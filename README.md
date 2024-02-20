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

Retrieving data from the YouTube API

The project utilizes the Google API to retrieve comprehensive data from YouTube channels. The data includes information on channels, playlists, videos, and comments. By interacting with the Google API, we collect the data and merge it into a JSON file.

Storing data in MongoDB

The retrieved data is stored in a MongoDB database based on user authorization. If the data already exists in the database, it can be overwritten with user consent. This storage process ensures efficient data management and preservation, allowing for seamless handling of the collected data.

Migrating data to a SQL data warehouse

The application allows users to migrate data from MongoDB to a SQL data warehouse. Users can choose which channel's data to migrate. To ensure compatibility with a structured format, the data is cleansed using the powerful pandas library. Following data cleaning, the information is segregated into separate tables, including channels, playlists, videos, and comments, utilizing SQL queries.

Finally, create a Dashboard by using Streamlit and give dropdown options on the Dashboard to the user and select a question from that menu to analyse the data and show the output in Dataframe Table

Video Link:

the project outcome is shown in the below link

LinkedIn: https://www.linkedin.com/posts/karthickraja1496_datascience-guvi-zenclass-activity-7165721077501874176-4fyG?utm_source=share&utm_medium=member_desktop
