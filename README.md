# Databricks Workspace Migration
Simple utility for migrating your artifacts (notebooks, folders, experiments) from one Databricks workspace to another

# How-To

### config.py   
Add your source workspace credentials and destination workspace credentials, including:   
1. **Workspace Shard URI**   
1. **Workspace Path** where artifacts are downloaded from (or uploaded to)   
1. **Worksapce Access Token**   
1. **Workspace User** login user of the workspace   
1. **Local Path** path for your artifacts on local drive   

### upload.py     
Upload artifacts to destination workspace from your local drive: `python upload.py`

### download.py 
Download artifacts from source workspace to your local drive: `python download.py`
