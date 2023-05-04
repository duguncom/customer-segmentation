# RFM
- The goal of this project is to develop and deploy a model for segmenting B2B customers 
​
## Owner Team
Data Science Team
​
​
# Table of Contents
- [churn](#churn)
- [Owner Team](#owner-team)
- [Description](#description)
- [Full Documentation](#full-documentation)
- [Project folders Structure](#project-folders-structure)
- [Setting up the environment](#setting-up-the-environment)
# Description
The goal of this project is to develop and deploy a model for ...
​

​
​
<a name="doc"></a>
# Full Documentation


## Project folders Structure
​
An overview of the project folders structure and description for each folder/file
​

```
churn
├── model
│   ├──model.pkl
├── data                              
├── notebooks   
│   ├── 01-get-data.ipynb                
│   ├── 02-features.ipynb               
│   ├── 03-EDA.ipynb               
│   ├── 04-Modeling.ipynb
│   ├── 05-evaluate.ipynb              
├── sql 
│   └── create_table.sql                                   
│   └── calls.sql                                        
├── src
│   └── utils.py
│   └── config.py
│   └── table_schema.json                          
├── environment.yaml                                                       
├── requirements.txt                         
├── Readme.md                                
```

**get-data.ipynb** : this is the notebook we use to get the necessary tables from database, SQL commands are in sql file and if any additional table is necessary it can be used easily

**features.ipynb** : this is the notebook to create our features we will be giving to model which includes 

**EDA.ipynb** : We use sweetviz library for visualization. sweetviz is an open-source Python library that generates beautiful, high-density visualizations to kickstart EDA (Exploratory Data Analysis) with just two lines of code. Output is a fully self-contained HTML application. Manual EDA has been performed to compliment the Sweetviz. 

**Modeling.ipyb** : Creating business logic

**evaluate.ipynb** : Final DF


<a name="Setting-up"></a>
# Setting up the environment

To run the project notebooks you need to setup the required packages within a virtual environment as follows.

* Create a conda virtual environment from yaml file `conda env create --file environment.yaml`
* You can verify that the new environment was correctly installed using `conda env list`
* Activate the new environment `conda activate RFM`
* Run `$CONDA_PREFIX/bin/jupyter lab`

To update existing environment using the environment yaml file, run the following command

```
conda env update --name churn --file environment.yaml --prune
```

<a name="download"></a>
