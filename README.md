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
### wip
This folder is not part of the end solution, it's merely a compilation of files that were used to test some of Typescript's functionalities as well as csv files with sampled data of some of the corner cases. The purpose was to better understand the language, create first drafts of the end solution, analyse input data to learn more about it's data types. I'm leaving these files in case they would help to gain some insights into my decisions, knowing that they are neither refined nor tidy.
## How to Configure and Run the Solution
## Development Notes: Insights and Solution Drawbacks
