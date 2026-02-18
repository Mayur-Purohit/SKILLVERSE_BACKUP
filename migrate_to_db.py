"""
Migration Script: Import data from txt files to database tables

This script reads existing wallet and transaction data from:
- wallets.txt (wallet balances)
- transactions.txt (transaction history)

And imports them into the new database tables:
- wallets (Wallet model)
- transactions (Transaction model)

Run this script ONCE after adding the new models to migrate data.

Usage: python migrate_to_db.py
"""

import json
import os
from datetime import datetime
from app import create_app
from models import db, User, Wallet, Transaction


def migrate_wallets(app):
    """
    Migrate wallet data from wallets.txt to the Wallet database table.
    """
    wallet_file = os.path.join(os.path.dirname(__file__), 'wallets.txt')
    
    if not os.path.exists(wallet_file):
        print("[SKIP] wallets.txt not found, skipping wallet migration")
        return 0
    
    count = 0
    skipped = 0
    with app.app_context():
        # Get all valid user IDs from DB
        valid_user_ids = set(u.id for u in User.query.all())
        
        with open(wallet_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    user_id = int(data.get('user_id', 0))
                    
                    # Skip if user doesn't exist in database
                    if user_id not in valid_user_ids:
                        print(f"  [SKIP] User {user_id} not found in database, skipping wallet")
                        skipped += 1
                        continue
                    
                    # Check if wallet already exists in DB
                    existing = Wallet.query.filter_by(user_id=user_id).first()
                    if existing:
                        print(f"  [SKIP] Wallet for user {user_id} already exists")
                        continue
                    
                    # Parse timestamps
                    created_at = None
                    last_updated = None
                    if data.get('created_at'):
                        try:
                            created_at = datetime.fromisoformat(data['created_at'])
                        except (ValueError, TypeError):
                            created_at = datetime.utcnow()
                    if data.get('last_updated'):
                        try:
                            last_updated = datetime.fromisoformat(data['last_updated'])
                        except (ValueError, TypeError):
                            last_updated = datetime.utcnow()
                    
                    wallet = Wallet(
                        user_id=user_id,
                        balance=float(data.get('balance', 0.0)),
                        created_at=created_at or datetime.utcnow(),
                        last_updated=last_updated or datetime.utcnow()
                    )
                    db.session.add(wallet)
                    count += 1
                    print(f"  [OK] Migrated wallet for user {user_id} (Balance: ₹{data.get('balance', 0)})")
                    
                except (json.JSONDecodeError, ValueError) as e:
                    print(f"  [ERROR] Error parsing wallet line: {e}")
                    continue
        
        try:
            db.session.commit()
            print(f"\n[OK] Successfully migrated {count} wallets")
        except Exception as e:
            db.session.rollback()
            print(f"\n[ERROR] Error committing wallets: {e}")
    
    return count


def migrate_transactions(app):
    """
    Migrate transaction data from transactions.txt to the Transaction database table.
    """
    txn_file = os.path.join(os.path.dirname(__file__), 'transactions.txt')
    
    if not os.path.exists(txn_file):
        print("[SKIP] transactions.txt not found, skipping transaction migration")
        return 0
    
    count = 0
    skipped = 0
    with app.app_context():
        # Get all valid user IDs from DB
        valid_user_ids = set(u.id for u in User.query.all())
        
        with open(txn_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    txn_id = data.get('id', '')
                    user_id = int(data.get('user_id', 0))
                    
                    # Skip if user doesn't exist in database
                    if user_id not in valid_user_ids:
                        print(f"  [SKIP] User {user_id} not found in database, skipping transaction {txn_id}")
                        skipped += 1
                        continue
                    
                    # Check if transaction already exists in DB
                    existing = Transaction.query.filter_by(
                        transaction_id=txn_id, 
                        user_id=user_id
                    ).first()
                    if existing:
                        print(f"  [SKIP] Transaction {txn_id} for user {user_id} already exists")
                        continue
                    
                    # Parse timestamp
                    timestamp = None
                    if data.get('timestamp'):
                        try:
                            timestamp = datetime.fromisoformat(data['timestamp'])
                        except (ValueError, TypeError):
                            timestamp = datetime.utcnow()
                    
                    txn = Transaction(
                        transaction_id=txn_id,
                        user_id=user_id,
                        username=data.get('username'),
                        amount=float(data.get('amount', 0)),
                        method=data.get('method', 'wallet'),
                        status=data.get('status', 'success'),
                        txn_type=data.get('type'),
                        description=data.get('description', ''),
                        new_balance=data.get('new_balance'),
                        txn_date=data.get('date', ''),
                        txn_time=data.get('time', ''),
                        timestamp=timestamp or datetime.utcnow()
                    )
                    db.session.add(txn)
                    count += 1
                    print(f"  [OK] Migrated transaction {txn_id} for user {user_id} (₹{data.get('amount', 0)})")
                    
                except (json.JSONDecodeError, ValueError) as e:
                    print(f"  [ERROR] Error parsing transaction line: {e}")
                    continue
        
        try:
            db.session.commit()
            print(f"\n[OK] Successfully migrated {count} transactions")
        except Exception as e:
            db.session.rollback()
            print(f"\n[ERROR] Error committing transactions: {e}")
    
    return count


if __name__ == '__main__':
    print("=" * 60)
    print("SkillVerse - Data Migration: TXT to Database")
    print("=" * 60)
    
    app = create_app()
    
    with app.app_context():
        # Create new tables if they don't exist
        db.create_all()
        print("[OK] Database tables created/verified\n")
    
    # Migrate wallets
    print("-" * 40)
    print("Migrating Wallets...")
    print("-" * 40)
    wallet_count = migrate_wallets(app)
    
    print()
    
    # Migrate transactions
    print("-" * 40)
    print("Migrating Transactions...")
    print("-" * 40)
    txn_count = migrate_transactions(app)
    
    print("\n" + "=" * 60)
    print(f"Migration Complete!")
    print(f"  Wallets migrated:      {wallet_count}")
    print(f"  Transactions migrated: {txn_count}")
    print("=" * 60)
    print("\nNote: The old wallets.txt and transactions.txt files are kept")
    print("as backups. You can safely delete them after verifying the data.")
