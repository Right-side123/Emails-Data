import os
import mailbox
import mysql.connector
from mysql.connector import Error
from email.utils import parsedate_to_datetime

DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'rightside@123',
    'database': 'emailsdata'
}

MBOX_PATH = 'D:/Download/All mail Including Spam and Trash-002 (1).mbox'

def process_mbox():
    if not os.path.exists(MBOX_PATH):
        print(f"MBOX file not found at: {MBOX_PATH}")
        return

    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        batch_size = 1000
        batch = []

        mbox = mailbox.mbox(MBOX_PATH)
        count = 0
        unique_pairs = set() 

        for message in mbox:
            sender = message.get('From', '').strip()
            receiver = message.get('To', '').strip()
            raw_date = message.get('Date', '')

            try:
                parsed_date = parsedate_to_datetime(raw_date)
                date = parsed_date.strftime('%Y-%m-%d %H:%M:%S')
            except:
                date = None

           
            pair = (sender, receiver)
            if pair in unique_pairs:
                continue
            unique_pairs.add(pair)

            batch.append((sender, receiver, date))
            count += 1

            if len(batch) >= batch_size:
                cursor.executemany('''
                    INSERT INTO uniqueemailsdata (sender, receiver, date)
                    VALUES (%s, %s, %s)
                ''', batch)
                conn.commit()
                print(f"Inserted {count} unique records so far...")
                batch.clear()

        if batch:
            cursor.executemany('''
                INSERT INTO uniqueemailsdata (sender, receiver, date)
                VALUES (%s, %s, %s)
            ''', batch)
            conn.commit()
            print(f"Final batch inserted. Total unique records: {count}")

    except Error as e:
        print(f"Database error: {e}")

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals() and conn.is_connected():
            conn.close()
        print("Done!")

if __name__ == "__main__":
    process_mbox()
