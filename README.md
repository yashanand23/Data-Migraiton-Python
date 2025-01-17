## Data Migration
This project is designed to perform data migration and reconciliation between a MongoDB source and a MySQL destination. The project follows the Extract, Transform, Load (ETL) process, ensuring that the migrated data is accurate and consistent between the two systems.

The project uses Python scripts, organized in the src/ directory, to manage the ETL pipeline and data reconciliation process. The main entry point for executing the data migration and reconciliation processes is main.py.

### Folder Structure
src/
├── data_extraction.py
├── data_transformation.py
├── data_loading.py
├── reconciliation.py
└── main.py

## Files
#### data_extraction.py
- Purpose: Connects to a MongoDB database to extract raw data from specified collections.
- Functionality: Fetches data and formats it into a structure suitable for further processing..

#### data_transformation.py
- Purpose: Cleans and transforms the raw data into a format compatible with the MySQL database schema.
- Functionality: Performs data cleaning, filtering, and validation.
Handles tasks like joining datasets, data type conversions, and enrichment.

#### data_loading.py
- Purpose: Loads the transformed data into the MySQL database.
- Functionality: Establishes a connection with MySQL.
Creates or updates necessary tables.
Inserts the processed data efficiently.

#### Reconciliation.py
- Purpose: Ensures data consistency between MongoDB and MySQL.
- Functionality: Compares the number of records between MongoDB collections and MySQL tables.
Logs mismatches and discrepancies to help identify potential issues.
Implements specific adjustments for tables like books (e.g., using DISTINCT for exploded tables).

#### main.py
This is the main script that orchestrates the ETL process. It sequentially calls the data extraction, transformation, loading and reconciliation functions in the proper order. Running this file initiates the data migration pipeline from MongoDB to MySQL.

## Getting Started

-To get started with this project, follow the steps below:

1. Clone the repository:
git clone <repository-url>
cd <repository-folder>/src

2. Set up the environment:
python -m venv venv
source venv/bin/activate  # On Windows, use venv\Scripts\activate

3. Install dependencies: Install the required Python packages by running:
pip install -r requirements.txt

4. Run the data migration pipeline:
python main.py

This will:

- Extract data from MongoDB.
- Transform and clean the data.
- Load the processed data into MySQL.
- Perform reconciliation to ensure data integrity.
