# Wallet by BudgetBakers to Cashew SQLite Migration

This script migrates the data from Wallet by BudgetBakers to Cashew SQLite database.

## Requirements
- Python 3.10
- requests

## Usage
1. Extract the data from Wallet by BudgetBakers
2. Migrate the data to Cashew SQLite database

### Extract the data from Wallet by BudgetBakers
```bash
pip install -r requirements.txt
python extract_wallet_data.py
```

### Migrate the data to Cashew SQLite database
```bash
pip install -r requirements.txt
python wallet_to_cashew_clean_migrator.py
```