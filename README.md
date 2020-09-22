# Databricks Workspace Migration
Simple utility for migrating your artifacts (notebooks, folders, experiments) from one Databricks workspace to another

# How-To

### config.py   
Add your Databricks workspace credentials to the `config.py`, concretely:   
1. **Workspace Shard URI** your Databricks workspace URI (e.g. https://adb-xxxxxx.azuredatabricks.net/?o=xxxxx)   
1. **Workspace Path** directory that artifacts are downloaded from (or uploaded to)   
1. **Worksapce Access Token** workspace access token   
1. **Workspace User** login user of the workspace (e.g. me@example.com)   
1. **Local Path** path for your artifacts on local disk (e.g. path/to/notebooks/)   

### upload.py     
Upload artifacts to destination workspace from your local drive: `python upload.py`

### download.py 
Download artifacts from source workspace to your local drive: `python download.py`
