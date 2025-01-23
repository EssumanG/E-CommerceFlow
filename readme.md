# E-Commerce FLow: Data Pipeline
A data engineering project for building an ETL pipeline, REST APIs, a web service using Apache Airflow, Apache Spark, PostgreSQL, React, and Flask.

## Table of Contents
1. [Project Overview](#project-overview)
2. [Features](#features)
3. [Tech Stack](#tech-stack)
4. [Folder Structure](#folder-structure)
5. [Setup and Installation](#setup-and-installation)
6. [Usage](#usage)
7. [Workflows](#workflows)
8. [Contributing](#contributing)
9. [License](#license)

---

## Project Overview

CommerceFlow is a modular microservices-based application designed to:
- Extract, transform, and load (ETL) e-commerce data.
- Build APIs to expose processed data.
- Create a React-based web service for data visualization.

This project demonstrates data engineering best practices using modern tools and frameworks.

---

## Features

- **ETL Pipeline**: Processes raw e-commerce data using Spark
- **Orchestration**: Airflow manages workflows, including triggering Spark jobs.
- **APIs**: Flask-based RESTful APIs for CRUD operations on processed data.
- **Frontend**: React web app to visualize data and interact with the APIs.

- **Containerized Deployment**: Fully containerized using Docker and Docker Compose.

---

## Tech Stack

- **Orchestration**: Apache Airflow  
- **Distributed Processing**: Apache Spark  
- **Backend**: Flask  
- **Frontend**: React  
- **Database**: PostgreSQL 
- **Containerization**: Docker  
- **Version Control**: Git  

---

## Project Status

🚧 **Project is Under Development** 🚧  

### Current Progress:
- **Apache Airflow**: Set up and tested using `BashOperators`.  
- **Folder Structure**: Established a modular folder structure for scalability.  

### Next Steps:
- Implement the **ETL pipeline** using Apache Spark.  
- Integrate **Apache Kafka** for streaming raw data.  
- Develop **Flask-based REST APIs** to expose processed data.  
- Build the **React web application** for data visualization.  
- Train and deploy the **machine learning model** for analytics.

---
## Setup and Installation

### Prerequisites

Before starting, ensure you have the following installed on your machine:
- **Docker** and **Docker Compose**
- **Git**

### Steps to Set Up the Project

1. **Clone the Repository**  
   Clone this repository to your local machine:
   ```bash
   git clone https://github.com/EssumanG/E-CommerceFlow.git
   cd commerceflow
### Set Up Environment Variables

1. Copy the example `.env` file to create your own environment configuration file:
   ```bash
   cp .env.example .env


### Build and Start the Containers

1. **Build the Docker Containers**  
   Use Docker Compose to build all the required services:
   ```bash
   docker-compose up --build

2. **Start the Containers**  
    The above command will automatically start all the services after building. If you need to start the services later without rebuilding, use:
     ```bash
   docker-compose up 

3. **In Detach Mode**
    ```bash
    docker-compose up 

4. **Verify Services**
Once the containers are running, confirm all services are operational by checking:
    
    - Airflow UI at http://localhost:8080.