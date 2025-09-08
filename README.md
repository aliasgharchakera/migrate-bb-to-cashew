# Wallet by BudgetBakers to Cashew SQLite Migration

This script migrates the data from Wallet by BudgetBakers to Cashew SQLite database.

## Requirements
- Python 3.10
- requests
- dotenv

## Setup
1. Create a `.env` file in the root of the project and add the following variables:
```bash
BUDGETBAKERS_DATABASE_ID=your_budgetbakers_database_id
BUDGETBAKERS_AUTH_TOKEN=your_budgetbakers_auth_token
```

2. Install the dependencies
```bash
pip install -r requirements.txt
```

## Usage
1. Extract the data from Wallet by BudgetBakers
2. Migrate the data to Cashew SQLite database

### Extract the data from Wallet by BudgetBakers
```bash
python extract_wallet_data.py
```

### Migrate the data to Cashew SQLite database
```bash
python wallet_to_cashew_migrator.py
```