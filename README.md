# Spotify Data Transformation and Analysis
This repository contains the solution for Telesoftas' task for the Cloud Engineer position.
Task description can be found in [task.pdf](task.pdf).

This document will cover the following topics:
- Repository structure
- How to configure and run the solution
- Development notes: insights and solution drawbacks

## Repository Structure
The repository contains 4 folders as well as some files not placed in any of the folders:

### root folder
Contains package files with described dependencies to installed packages and Jest configuration together with the task description and gitignore file.

### SQL_preparation
Contains scripts for setting up a workspace in `PostgreSQL` (creating a database, a user, granting privileges, creating schemas) and creating tables to which data should be imported. Additionally, the `load_tracks.sql` contains code used to attempt to import data downloaded from S3 and later on some scripts for inserting test data to use as input for the analysis views.

### analysis_views
3 sql files for each of the views created based on the requirements in the task.

### data_transformation
Contains the Typescript solution for downloading artists and tracks CSV files from Kaggle (`fileProcessing.ts`, `fileHandling.ts`), applying transformations (`transformations.ts`) and uploading the changed files to AWS S3 (`fileProcessing.ts`, `fileHandling.ts`). The configurations, such as AWS bucket name or file URLs/names, are stored in `config.ts`. The solution is run from `main.ts`.
**When setting up this folder is where the `.env` file should be placed.**
This folder also contains a subfolder (`__tests__`) that contains Jest unit tests for the functions described in `transformations.ts`.

### wip
This folder is not part of the end solution, it's merely a compilation of files that were used to test some of Typescript's functionalities as well as csv files with sampled data of some of the corner cases. The purpose was to better understand the language, create first drafts of the end solution, analyse input data to learn more about it's data types. I'm leaving these files in case they would help to gain some insights into my decisions, knowing that they are neither refined nor tidy.

## How to Configure and Run the Solution
The homework solution is split into 2 parts: Typescript scripts for data transformation and SQL scripts for data analysis.
### Typescript / Node.js
#### Configuration
The first step to do is to create a `.env` file in the `data_transformation` folder. The file should contain an S3 access key id and a secret access key for the bucket that will be used to store the output of the script.
The file should have the following values:
```
S3_ACCESS_KEY_ID=accessKeyIdValue
S3_SECRET_ACCESS_KEY=secretAccessKeyValue
```
The values should be plain strings without any quotes. The user which's credentials will be used should have permission policies assigned that would enable the user to upload data to the S3 bucket of choice. If the user doesn't have an access key, it can be generated in IAM under `Users -> Security Credentials -> Access keys` (I've personally used the option for "Local code"). 

After creating the `.env` file and placing it in the right folder, `config.ts` in the same folder should be updated by changing the value of `S3_BUCKET_NAME` with the bucket that will be used and to which the user which's credentials were saved has necessary permissions.
#### Execution
Make sure you have typescript installed (for example by running `tsc --version`) then change directory to the `data_transformation` folder. Compile JavaScript files by running `tsc main.ts` and then run the solution with `node main.js`. If it's successful, messages logging the progress of the solution will be printed out in the console.
### Test execution
If there's a need to run unit tests that can be done by running the whole test suite with `npm test` (directory can be any folder in the repository). 

### PostgreSQL
Download the files from S3 that were uploaded with the Typescript solution.
#### Configuration
Login to your PostgreSQL server with your credentials. If needed, create a database and user for this solution, if they're already exist, create two schemas: `music` (for `tracks` and `artists` tables) and `views` (for `analysis` views). Use `setup_workspace.sql` to achieve that.
The schema creation is optional, already existing schemas (or just the default `public` schema) can be used, if other scripts are adjusted accordingly.
Go to your preferred (or created) database and create empty tables using the code in `create_tables.sql`.
Now, ideally, the tracks and artists files would be places in `C:/Program Files/PostgreSQL/<version>/data/<json_name.json>` and a script similar to the one found in the first half of `load_tracks.sql` would be used to load the data. Sadly, it doesn't work as the format in which the json arrays are saved is not the same as it would be if the arrays were saved in CSV (or the format that would be expected by the `COPY` command). In the interest of time, I've decided not to dig in deeper into how I could either change the way the JSON files were saved or save the data in a CSV format or search for another solution that might work with my file structure and provided some test data that should cover the analysis scenarios from the task. The `INSERT` statements for these can be found in the second half of the `load_tracks.sql` file.
#### Execution
Each of the files in `analysis_views` cover a scenario described in the task. 
1. `track_overview`
2. `followed_artist_tracks`
3. `yearly_most_energetic_track`

Run each file's content in pgAdmin or your tool of choice that would be appropriate for the task. Write a simple `SELECT` statement (`SELECT * FROM views.<view_name>`) to view the contents of the view. *NB:* if the actual data (not the test sample) is loaded, it might be a good idea to limit the number of rows returned by the `SELECT`.

## Development Notes: Insights and Solution Drawbacks

Thank you if you've read till the end!