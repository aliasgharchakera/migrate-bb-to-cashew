#!/usr/bin/env python3
"""
Wallet by BudgetBakers Data Extractor
Extracts all data from the CouchDB backend for migration to Cashew
"""

import requests
import json
import time
from typing import Dict, List, Any
import base64
import os
import dotenv

dotenv.load_dotenv()

BUDGETBAKERS_DATABASE_ID = os.getenv('BUDGETBAKERS_DATABASE_ID')
BUDGETBAKERS_AUTH_TOKEN = os.getenv('BUDGETBAKERS_AUTH_TOKEN')

class WalletDataExtractor:
    def __init__(self):
        self.base_url = f"https://couch-prod-asia-1.budgetbakers.com/{BUDGETBAKERS_DATABASE_ID}"
        self.headers = {
            'authorization': f'Basic {BUDGETBAKERS_AUTH_TOKEN}',
        }


    def get_changes(self, since=0, limit=1000):
        """Get changes from CouchDB"""
        url = f"{self.base_url}/_changes"
        params = {
            'timeout': 10000,
            'style': 'main_only',
            'since': since,
            'limit': limit
        }
        
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()

    def get_document(self, doc_id, rev):
        """Get a specific document"""
        url = f"{self.base_url}/_bulk_get"
        params = {'revs': 'true', 'latest': 'true'}
        
        data = {
            "docs": [{"id": doc_id, "rev": rev}]
        }
        
        response = requests.post(url, headers=self.headers, params=params, json=data)
        response.raise_for_status()
        return response.json()

    def get_all_documents(self, batch_size=50):
        """Get all documents in batches"""
        print("Getting all document IDs...")
        changes = self.get_changes(limit=10000)  # Get all changes
        
        all_docs = []
        doc_ids = []
        
        for change in changes['results']:
            doc_id = change['id']
            rev = change['changes'][0]['rev']
            doc_ids.append((doc_id, rev))
        
        print(f"Found {len(doc_ids)} documents")
        
        # Process in batches
        for i in range(0, len(doc_ids), batch_size):
            batch = doc_ids[i:i + batch_size]
            print(f"Processing batch {i//batch_size + 1}/{(len(doc_ids) + batch_size - 1)//batch_size}")
            
            # Prepare batch request
            docs_data = [{"id": doc_id, "rev": rev} for doc_id, rev in batch]
            data = {"docs": docs_data}
            
            try:
                response = requests.post(
                    f"{self.base_url}/_bulk_get",
                    headers=self.headers,
                    params={'revs': 'true', 'latest': 'true'},
                    json=data
                )
                response.raise_for_status()
                batch_result = response.json()
                all_docs.extend(batch_result['results'])
                
                # Small delay to avoid overwhelming the server
                time.sleep(0.1)
                
            except Exception as e:
                print(f"Error processing batch {i//batch_size + 1}: {e}")
                continue
        
        return all_docs

    def categorize_documents(self, all_docs):
        """Categorize documents by type"""
        categories = {
            'transactions': [],
            'debts': [],
            'accounts': [],
            'categories': [],
            'currencies': [],
            'hashtags': [],
            'budgets': [],
            'other': []
        }
        
        for doc_result in all_docs:
            if 'docs' in doc_result and doc_result['docs']:
                doc = doc_result['docs'][0].get('ok', {})
                doc_id = doc.get('_id', '')
                
                if doc_id.startswith('Record_'):
                    categories['transactions'].append(doc)
                elif doc_id.startswith('-Debt_'):
                    categories['debts'].append(doc)
                elif doc_id.startswith('-Account_'):
                    categories['accounts'].append(doc)
                elif doc_id.startswith('-Category_'):
                    categories['categories'].append(doc)
                elif doc_id.startswith('-Currency_'):
                    categories['currencies'].append(doc)
                elif doc_id.startswith('-HashTag_'):
                    categories['hashtags'].append(doc)
                elif doc_id.startswith('-Budget_'):
                    categories['budgets'].append(doc)
                elif doc_id.startswith('-Notification_'):
                    continue
                else:
                    categories['other'].append(doc)
        
        return categories

    def save_data(self, data, filename):
        """Save data to JSON file"""
        with open(f"output/{filename}", 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"Saved {len(data)} items to {filename}")

    def extract_all_data(self):
        """Main extraction function"""
        print("Starting Wallet by BudgetBakers data extraction...")
        
        # Get all documents
        all_docs = self.get_all_documents()
        
        # Categorize documents
        categorized = self.categorize_documents(all_docs)
        
        # Save categorized data
        for category, docs in categorized.items():
            if docs:
                filename = f"wallet_{category}.json"
                self.save_data(docs, filename)
                print(f"{category}: {len(docs)} documents")
        
        # Save raw data
        self.save_data(all_docs, "wallet_all_data.json")
        
        return categorized

if __name__ == "__main__":
    extractor = WalletDataExtractor()
    data = extractor.extract_all_data()
    print("Data extraction completed!")
