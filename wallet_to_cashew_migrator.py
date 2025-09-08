#!/usr/bin/env python3
"""
Wallet by BudgetBakers to Cashew SQLite Migration Script (CLEAN VERSION)
Creates a fresh Cashew database with only the migrated Wallet data
"""

import json
import sqlite3
import uuid
import time
import os
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional

class WalletToCashewCleanMigrator:
    def __init__(self, cashew_db_path: str):
        self.cashew_db_path = cashew_db_path
        self.accounts = {}
        self.categories = {}
        self.currencies = {}
        self.debts = []
        self.transactions = []
        
        # Create a fresh database
        if os.path.exists(cashew_db_path):
            os.remove(cashew_db_path)
        
        self.conn = sqlite3.connect(cashew_db_path)
        self.cursor = self.conn.cursor()
        
        # Create the database schema
        self.create_schema()
        
        # Mapping for Wallet categories to Cashew categories
        self.category_mapping = {
            'Food & Dining': 'Dining',
            'Groceries': 'Groceries', 
            'Shopping': 'Shopping',
            'Transportation': 'Transit',
            'Entertainment': 'Entertainment',
            'Bills & Utilities': 'Bills & Fees',
            'Gifts & Donations': 'Gifts',
            'Personal Care': 'Beauty',
            'Work & Business': 'Work',
            'Travel': 'Travel',
            'Income': 'Income'
        }
        
    def create_schema(self):
        """Create the Cashew database schema from schema file"""
        print("Creating fresh Cashew database schema...")
        
        # Read and execute the schema file
        with open('cashew_schema.sql', 'r', encoding='utf-8') as f:
            schema_sql = f.read()
        
        # Split by semicolon and execute each statement
        statements = [stmt.strip() for stmt in schema_sql.split(';') if stmt.strip()]
        
        for statement in statements:
            if statement and not statement.startswith('CREATE TABLE sqlite_sequence'):
                self.cursor.execute(statement)
        
        # Load default app settings
        with open('cashew_app_settings.json', 'r', encoding='utf-8') as f:
            default_settings = f.read()
        
        self.cursor.execute("""
            INSERT INTO app_settings (settings_j_s_o_n, date_updated)
            VALUES (?, ?)
        """, (default_settings, int(time.time())))
        
        self.conn.commit()
        print("✅ Fresh Cashew database schema created from cashew_schema.sql")
        
    def load_data(self):
        """Load all extracted Wallet data"""
        print("Loading extracted Wallet data...")
        
        # Load accounts
        with open('output/wallet_accounts.json', 'r', encoding='utf-8') as f:
            accounts_data = json.load(f)
            for account in accounts_data:
                self.accounts[account['_id']] = account
        
        # Load categories
        with open('output/wallet_categories.json', 'r', encoding='utf-8') as f:
            categories_data = json.load(f)
            for category in categories_data:
                self.categories[category['_id']] = category
        
        # Load currencies
        with open('output/wallet_currencies.json', 'r', encoding='utf-8') as f:
            currencies_data = json.load(f)
            for currency in currencies_data:
                self.currencies[currency['_id']] = currency
        
        # Load debts
        with open('output/wallet_debts.json', 'r', encoding='utf-8') as f:
            self.debts = json.load(f)
        
        # Load transactions
        with open('output/wallet_transactions.json', 'r', encoding='utf-8') as f:
            self.transactions = json.load(f)
        
        print(f"Loaded: {len(self.accounts)} accounts, {len(self.categories)} categories, {len(self.debts)} debts, {len(self.transactions)} transactions")
    
    def generate_id(self) -> str:
        """Generate a UUID for Cashew database"""
        return str(uuid.uuid4())
    
    def timestamp_to_unix(self, timestamp) -> int:
        """Convert Wallet timestamp to Unix timestamp"""
        if isinstance(timestamp, str):
            # Handle ISO date strings
            try:
                from datetime import datetime
                dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                return int(dt.timestamp())
            except:
                return int(time.time())
        elif isinstance(timestamp, (int, float)):
            # Wallet uses milliseconds, convert to seconds
            return int(timestamp // 1000)
        else:
            return int(time.time())
    
    def format_amount(self, amount: float) -> float:
        """Format amount for Cashew - remove decimal places (Wallet by BudgetBakers includes 2 decimals)"""
        # Wallet by BudgetBakers amounts include 2 decimal places, so we need to divide by 100
        # to get the actual amount for Cashew
        return float(amount) / 100.0
    
    def create_wallets(self):
        """Create wallets in Cashew database"""
        print("Creating wallets...")
        
        wallet_order = 0
        
        for account_id, account in self.accounts.items():
            if account.get('deleted', False):
                continue
                
            wallet_pk = self.generate_id()
            name = account.get('name', 'Unknown Account')
            currency = account.get('currency', 'PKR')
            
            # Map currency format
            currency_format = f"[0,1]" if currency == 'PKR' else f"[0,1]"
            
            self.cursor.execute("""
                INSERT INTO wallets (wallet_pk, name, colour, icon_name, date_created, 
                                   date_time_modified, "order", currency, currency_format, 
                                   decimals, home_page_widget_display)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                wallet_pk,
                name,
                None,  # colour
                None,  # icon_name
                self.timestamp_to_unix(account.get('created', int(time.time() * 1000))),
                1757158578,  # date_time_modified
                wallet_order,
                currency.lower(),
                currency_format,
                2,  # decimals
                None  # home_page_widget_display
            ))
            
            # Store mapping for later use
            account['cashew_wallet_pk'] = wallet_pk
            wallet_order += 1
        
        self.conn.commit()
        print(f"Created {len([a for a in self.accounts.values() if not a.get('deleted', False)])} wallets")
    
    def create_categories(self):
        """Create categories in Cashew database"""
        print("Creating categories...")
        
        category_order = 0
        
        for category_id, category in self.categories.items():
            if category.get('deleted', False):
                continue
                
            category_pk = self.generate_id()
            name = category.get('name', 'Unknown Category')
            
            # Map to existing Cashew category if possible
            mapped_name = self.category_mapping.get(name, name)
            
            # Determine if it's income or expense
            is_income = 1 if category.get('type') == 'income' else 0
            
            self.cursor.execute("""
                INSERT INTO categories (category_pk, name, colour, icon_name, emoji_icon_name,
                                      date_created, date_time_modified, "order", income, 
                                      method_added, main_category_pk)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                category_pk,
                mapped_name,
                None,  # colour
                None,  # icon_name
                None,  # emoji_icon_name
                self.timestamp_to_unix(category.get('created', int(time.time() * 1000))),
                1757158578,  # date_time_modified
                category_order,
                is_income,
                None,  # method_added
                None   # main_category_pk
            ))
            
            # Store mapping for later use
            category['cashew_category_pk'] = category_pk
            category_order += 1
        
        self.conn.commit()
        print(f"Created {len([c for c in self.categories.values() if not c.get('deleted', False)])} categories")
    
    def create_objectives_from_debts(self):
        """Convert Wallet debts to Cashew objectives (loans) following Cashew's loan system"""
        print("Converting debts to objectives (loans)...")
        
        objective_order = 0
        
        for debt in self.debts:
            if debt.get('deleted', False):
                continue
                
            objective_pk = self.generate_id()
            name = debt.get('name', 'Unknown Debt')
            amount = self.format_amount(abs(float(debt.get('amount', 0))))
            
            # According to Cashew documentation:
            # - Loans lent out (type=1) are recorded as income objectives
            # - Loans borrowed (type=0) are recorded as expense objectives
            debt_type = debt.get('type', 0)
            is_income = 1 if debt_type == 1 else 0  # type=1 means lent out (income)
            
            # Get wallet reference (accountId -> wallet_fk)
            account_id = debt.get('accountId')
            wallet_pk = '0'  # Default wallet
            if account_id and account_id in self.accounts:
                wallet_pk = self.accounts[account_id].get('cashew_wallet_pk', '0')
            
            # Calculate end date if available
            end_date = None
            if debt.get('payBackTime'):
                end_date = self.timestamp_to_unix(debt['payBackTime'])
            
            self.cursor.execute("""
                INSERT INTO objectives (objective_pk, type, name, amount, "order", colour,
                                      date_created, end_date, date_time_modified, icon_name,
                                      emoji_icon_name, income, pinned, archived, wallet_fk)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                objective_pk,
                0,  # type (0 = objective/loan)
                name,
                amount,
                objective_order,
                None,  # colour
                self.timestamp_to_unix(debt.get('date', int(time.time() * 1000))),
                end_date,
                1757158578,  # date_time_modified
                None,  # icon_name
                None,  # emoji_icon_name
                is_income,
                1,  # pinned
                0,  # archived
                wallet_pk
            ))
            
            # Store mapping for later use
            debt['cashew_objective_pk'] = objective_pk
            objective_order += 1
        
        self.conn.commit()
        print(f"Created {len([d for d in self.debts if not d.get('deleted', False)])} objectives from debts")
    
    def create_transactions(self):
        """Create transactions in Cashew database with proper debt linking"""
        print("Creating transactions...")
        
        # Create a mapping of debt IDs to objective IDs
        debt_to_objective = {}
        for debt in self.debts:
            if not debt.get('deleted', False) and 'cashew_objective_pk' in debt:
                debt_to_objective[debt['_id']] = debt['cashew_objective_pk']
        
        # Track associated titles for the associated_titles table
        associated_titles = {}  # title -> category_pk mapping
        
        for transaction in self.transactions:
            if transaction.get('deleted', False):
                continue
                
            transaction_pk = self.generate_id()
            
            # Extract payee and note from transaction
            payee = transaction.get('payee', '')
            note = transaction.get('note', '')
            
            # Set title: use payee if available, otherwise use note
            title = payee if payee else note
            if not title:
                title = 'Transaction'
            
            # Set note: use the note field, or empty if no note
            transaction_note = note if note else ''
            
            amount = self.format_amount(float(transaction.get('amount', 0)))
            
            # Get category reference
            category_id = transaction.get('categoryId')
            category_pk = '1'  # Default to first category
            if category_id and category_id in self.categories:
                category_pk = self.categories[category_id].get('cashew_category_pk', '1')
            
            # Get wallet reference (accountId -> wallet_fk)
            account_id = transaction.get('accountId')
            wallet_pk = '0'  # Default wallet
            if account_id and account_id in self.accounts:
                wallet_pk = self.accounts[account_id].get('cashew_wallet_pk', '0')
            
            # Determine if it's income
            is_income = 1 if amount > 0 else 0
            
            # Check if this transaction is linked to a debt via refObjects
            objective_pk = None
            objective_loan_pk = None
            
            if 'refObjects' in transaction and transaction['refObjects']:
                for ref_obj in transaction['refObjects']:
                    if ref_obj['id'] in debt_to_objective:
                        objective_pk = debt_to_objective[ref_obj['id']]
                        objective_loan_pk = debt_to_objective[ref_obj['id']]
                        break
            
            self.cursor.execute("""
                INSERT INTO transactions (transaction_pk, paired_transaction_fk, name, amount, note,
                                        category_fk, sub_category_fk, wallet_fk, date_created,
                                        date_time_modified, original_date_due, income, period_length,
                                        reoccurrence, end_date, upcoming_transaction_notification,
                                        type, paid, created_another_future_transaction, skip_paid,
                                        method_added, transaction_owner_email, transaction_original_owner_email,
                                        shared_key, shared_old_key, shared_status, shared_date_updated,
                                        shared_reference_budget_pk, objective_fk, objective_loan_fk,
                                        budget_fks_exclude)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                transaction_pk,
                None,  # paired_transaction_fk
                title,  # name (title)
                amount,
                transaction_note,  # note
                category_pk,
                None,  # sub_category_fk
                wallet_pk,
                self.timestamp_to_unix(transaction.get('date', int(time.time() * 1000))),
                1757158578,  # date_time_modified
                self.timestamp_to_unix(transaction.get('date', int(time.time() * 1000))),  # original_date_due
                is_income,
                None,  # period_length
                None,  # reoccurrence
                None,  # end_date
                1,  # upcoming_transaction_notification
                None,  # type
                1,  # paid (assuming all historical transactions are paid)
                0,  # created_another_future_transaction
                0,  # skip_paid
                None,  # method_added
                None,  # transaction_owner_email
                None,  # transaction_original_owner_email
                None,  # shared_key
                None,  # shared_old_key
                None,  # shared_status
                None,  # shared_date_updated
                None,  # shared_reference_budget_pk
                objective_pk,
                objective_loan_pk,
                None   # budget_fks_exclude
            ))
            
            # Track this title for associated_titles table
            if title and title != 'Transaction':
                associated_titles[title] = category_pk
        
        self.conn.commit()
        print(f"Created {len([t for t in self.transactions if not t.get('deleted', False)])} transactions")
        
        # Create associated_titles entries
        self.create_associated_titles(associated_titles)
    
    def create_associated_titles(self, associated_titles):
        """Create associated_titles entries for automatic categorization"""
        print("Creating associated titles...")
        
        # Get existing associated titles to avoid duplicates
        self.cursor.execute("SELECT title FROM associated_titles")
        existing_titles = {row[0] for row in self.cursor.fetchall()}
        
        # Get existing category order for associated titles
        self.cursor.execute("SELECT MAX(\"order\") FROM associated_titles")
        max_order = self.cursor.fetchone()[0] or 0
        
        order_counter = max_order + 1
        created_count = 0
        
        for title, category_pk in associated_titles.items():
            # Skip if title already exists
            if title in existing_titles:
                continue
            
            associated_title_pk = self.generate_id()
            
            self.cursor.execute("""
                INSERT INTO associated_titles (associated_title_pk, category_fk, title, date_created,
                                            date_time_modified, "order", is_exact_match)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                associated_title_pk,
                category_pk,
                title,
                int(time.time()),
                1757158578,  # date_time_modified
                order_counter,
                0  # is_exact_match (0 = false)
            ))
            
            order_counter += 1
            created_count += 1
        
        self.conn.commit()
        print(f"Created {created_count} associated titles")
    
    def generate_debt_summary(self):
        """Generate comprehensive debt summary with all associated records"""
        print("\n" + "="*80)
        print("COMPREHENSIVE DEBT SUMMARY")
        print("="*80)
        
        # Create debt summary file
        with open('output/debt_summary.txt', 'w', encoding='utf-8') as f:
            f.write("WALLET BY BUDGETBAKERS TO CASHEW DEBT MIGRATION SUMMARY\n")
            f.write("="*80 + "\n\n")
            
            # Get active debts
            active_debts = [d for d in self.debts if not d.get('deleted', False)]
            
            # Group debts by type
            borrowed_debts = [d for d in active_debts if d.get('type') == 0]
            lent_debts = [d for d in active_debts if d.get('type') == 1]
            
            # Calculate totals
            total_debt_amount = sum(self.format_amount(abs(float(d.get('amount', 0)))) for d in active_debts)
            borrowed_amount = sum(self.format_amount(abs(float(d.get('amount', 0)))) for d in borrowed_debts)
            lent_amount = sum(self.format_amount(abs(float(d.get('amount', 0)))) for d in lent_debts)
            
            f.write(f"OVERVIEW:\n")
            f.write(f"  Total Debts: {len(active_debts)}\n")
            f.write(f"  Money Borrowed: {len(borrowed_debts)} debts ({borrowed_amount:,.2f} PKR)\n")
            f.write(f"  Money Lent: {len(lent_debts)} debts ({lent_amount:,.2f} PKR)\n")
            f.write(f"  Total Amount: {total_debt_amount:,.2f} PKR\n\n")
            
            # Detailed debt analysis
            f.write("DETAILED DEBT ANALYSIS:\n")
            f.write("-" * 50 + "\n\n")
            
            for i, debt in enumerate(active_debts, 1):
                debt_id = debt['_id']
                name = debt.get('name', 'Unknown')
                amount = self.format_amount(abs(float(debt.get('amount', 0))))
                debt_type = debt.get('type', 0)
                type_text = "BORROWED" if debt_type == 0 else "LENT"
                
                # Get account info
                account_id = debt.get('accountId')
                account_name = "Unknown Account"
                if account_id and account_id in self.accounts:
                    account_name = self.accounts[account_id].get('name', 'Unknown Account')
                
                # Get dates
                created_date = debt.get('date', 'Unknown')
                payback_date = debt.get('payBackTime', 'Not set')
                paid_back = debt.get('paidBack', False)
                
                f.write(f"{i}. {name} ({type_text})\n")
                f.write(f"   Amount: {amount:,.2f} PKR\n")
                f.write(f"   Account: {account_name}\n")
                f.write(f"   Created: {created_date}\n")
                f.write(f"   Payback Date: {payback_date}\n")
                f.write(f"   Paid Back: {'Yes' if paid_back else 'No'}\n")
                f.write(f"   Debt ID: {debt_id}\n")
                
                # Find associated transactions
                associated_transactions = []
                for transaction in self.transactions:
                    if not transaction.get('deleted', False) and 'refObjects' in transaction and transaction['refObjects']:
                        for ref_obj in transaction['refObjects']:
                            if ref_obj['id'] == debt_id:
                                associated_transactions.append(transaction)
                                break
                
                f.write(f"   Associated Transactions: {len(associated_transactions)}\n")
                
                if associated_transactions:
                    f.write(f"   Transaction Details:\n")
                    for j, trans in enumerate(associated_transactions[:5], 1):  # Show first 5 transactions
                        trans_amount = self.format_amount(float(trans.get('amount', 0)))
                        trans_note = trans.get('note', 'No note')
                        trans_date = trans.get('date', 'Unknown date')
                        f.write(f"     {j}. {trans_note} - {trans_amount:,.2f} PKR ({trans_date})\n")
                    
                    if len(associated_transactions) > 5:
                        f.write(f"     ... and {len(associated_transactions) - 5} more transactions\n")
                
                f.write(f"   Cashew Objective ID: {debt.get('cashew_objective_pk', 'Not created')}\n")
                f.write("\n")
            
            # Summary statistics
            f.write("MIGRATION STATISTICS:\n")
            f.write("-" * 30 + "\n")
            f.write(f"Total Wallets Created: {len([a for a in self.accounts.values() if not a.get('deleted', False)])}\n")
            f.write(f"Total Categories Created: {len([c for c in self.categories.values() if not c.get('deleted', False)])}\n")
            f.write(f"Total Objectives Created: {len(active_debts)}\n")
            f.write(f"Total Transactions Created: {len([t for t in self.transactions if not t.get('deleted', False)])}\n")
            
            # Count debt-linked transactions
            debt_linked_transactions = 0
            for transaction in self.transactions:
                if not transaction.get('deleted', False) and 'refObjects' in transaction and transaction['refObjects']:
                    for ref_obj in transaction['refObjects']:
                        if any(ref_obj['id'] == debt['_id'] for debt in active_debts):
                            debt_linked_transactions += 1
                            break
            
            f.write(f"Debt-Linked Transactions: {debt_linked_transactions}\n")
            f.write(f"Migration Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        print("✅ Comprehensive debt summary saved to debt_summary.txt")
    
    def generate_migration_summary(self):
        """Generate a summary of the migration"""
        print("\n" + "="*60)
        print("CLEAN MIGRATION SUMMARY")
        print("="*60)
        
        # Count active items
        active_accounts = len([a for a in self.accounts.values() if not a.get('deleted', False)])
        active_categories = len([c for c in self.categories.values() if not c.get('deleted', False)])
        active_debts = len([d for d in self.debts if not d.get('deleted', False)])
        active_transactions = len([t for t in self.transactions if not t.get('deleted', False)])
        
        print(f"✅ Wallets created: {active_accounts}")
        print(f"✅ Categories created: {active_categories}")
        print(f"✅ Objectives (loans) created: {active_debts}")
        print(f"✅ Transactions created: {active_transactions}")
        
        # Debt summary with proper categorization
        total_debt_amount = sum(self.format_amount(abs(float(d.get('amount', 0)))) for d in self.debts if not d.get('deleted', False))
        borrowed_amount = sum(self.format_amount(abs(float(d.get('amount', 0)))) for d in self.debts if not d.get('deleted', False) and d.get('type') == 0)
        lent_amount = sum(self.format_amount(abs(float(d.get('amount', 0)))) for d in self.debts if not d.get('deleted', False) and d.get('type') == 1)
        
        print(f"\n💰 DEBT SUMMARY:")
        print(f"   Total debt amount: {total_debt_amount:,.2f} PKR")
        print(f"   Money borrowed (type=0): {borrowed_amount:,.2f} PKR")
        print(f"   Money lent (type=1): {lent_amount:,.2f} PKR")
        
        # Count debt-linked transactions
        debt_linked_transactions = 0
        for transaction in self.transactions:
            if not transaction.get('deleted', False) and 'refObjects' in transaction and transaction['refObjects']:
                for ref_obj in transaction['refObjects']:
                    if any(ref_obj['id'] == debt['_id'] for debt in self.debts if not debt.get('deleted', False)):
                        debt_linked_transactions += 1
                        break
        
        print(f"\n🔗 DEBT-LINKED TRANSACTIONS:")
        print(f"   Transactions linked to debts: {debt_linked_transactions}")
        
        print(f"\n📁 OUTPUT FILES:")
        print(f"   Clean Cashew Database: {self.cashew_db_path}")
        print(f"   Comprehensive Debt Summary: debt_summary.txt")
        
        print("\n🎉 CLEAN MIGRATION COMPLETED SUCCESSFULLY!")
        print("✅ Fresh database with only your Wallet by BudgetBakers data")
        print("✅ No old Cashew data - clean slate")
        print("✅ Debts properly converted to objectives (loans)")
        print("✅ Transactions properly linked to debts via refObjects")
        print("✅ Amounts formatted to 2 decimal places")
        print("✅ Account IDs properly mapped to wallet_fk")
        print("✅ Comprehensive debt summary with all associated records")
        print("You can now import this clean Cashew database into your app.")
        print("="*60)
    
    def migrate(self):
        """Run the complete migration process"""
        print("Starting CLEAN Wallet by BudgetBakers to Cashew SQLite migration...")
        print("Creating fresh database with only your migrated data")
        
        try:
            self.load_data()
            self.create_wallets()
            self.create_categories()
            self.create_objectives_from_debts()
            self.create_transactions()
            self.generate_debt_summary()
            self.generate_migration_summary()
            
        except Exception as e:
            print(f"❌ Migration failed: {str(e)}")
            self.conn.rollback()
            raise
        finally:
            self.conn.close()

if __name__ == "__main__":
    migrator = WalletToCashewCleanMigrator("wallet-to-cashew.sql")
    migrator.migrate()
