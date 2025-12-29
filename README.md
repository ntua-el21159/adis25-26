# adis25-26
2. Comparison between text-to-SQL methods
With the advent of Large Language models (LLMs), the task of translating natural
language into SQL (text-to-SQL) has gained significant attention in the Database
community. LLMs such as Qwen, Tiny Llama, GPT, etc., have shown superior
performance in converting text into SQL queries. However, for complex queries
the performance tends to drop. As part of this work, you are asked to compare
how LLMs perform when generating both simple and complex SQL queries from
a text for a Relational Database Management System (RDBMS). Additionally,
you are required to develop and scale code to create an agent that integrates
LLMs with an RDBMS and compare the performance across different types of
queries. This entails the following aspects that must be tackled by you:
1) Installation and setup of two specific systems: Using local or cloud
resources you are asked to successfully install and setup the LMM
environment and the RDBMS environment.
2) Data Generation: Using existing online datasets for text-to-SQL and
creating your own with more complex SQL queries, you should identify the
data that will be used for testing the system.
3) Code for measuring performance: Using (python) scripts you must
implement a small framework that acts as an agent for natural language
input, generated SQL queries, and execution on the RDBMS. The results
produced by the system should then be used to compare the performance
of different LLMs in generating queries. Performance must be evaluated in
terms of accuracy (correctness of generated queries) and computational
efficiency (time and resources used). Each team is encouraged to go
beyond the minimum requirements by designing additional experiments,
such as introducing multiple levels of query complexity. In this step, teams
should ensure that comparisons are based on meaningful and statistically
valid results.

# 1. Run setup script
.\setup.ps1

# OR manual steps:

# 2. Download datasets
python scripts\download_datasets.py

# 3. Extract schemas
python scripts\extract_schemas.py

# 4. Start databases
.\start_databases.bat
# OR: docker-compose up -d

# 5. Test connections
python scripts\test_connections.py

# 6. Test datasets
python scripts\test_datasets.py 
