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

        mbox = mailbox.mbox(MBOX_PATH)
        used_senders = set()
        used_receivers = set()
        count = 0

        for message in mbox:
            sender = message.get('From', '').strip()
            receiver = message.get('To', '').strip()
            raw_date = message.get('Date', '')

            try:
                parsed_date = parsedate_to_datetime(raw_date)
                date = parsed_date.strftime('%Y-%m-%d %H:%M:%S')
            except:
                date = None

            #  Skip if sender is already a receiver OR receiver already a sender
            if sender in used_receivers or receiver in used_senders:
                continue

            #  Case 1: Both are new
            if sender not in used_senders and receiver not in used_receivers:
                cursor.execute("""
                    INSERT INTO uniqueemailsnotnullreciever (sender, receiver, date)
                    VALUES (%s, %s, %s)
                """, (sender, receiver, date))
                used_senders.add(sender)
                used_receivers.add(receiver)
                count += 1

            #  Case 2: New sender only
            elif sender not in used_senders and receiver in used_receivers:
                cursor.execute("""
                    SELECT id FROM uniqueemailsnotnullreciever
                    WHERE receiver = %s AND sender IS NULL
                    LIMIT 1
                """, (receiver,))
                row = cursor.fetchone()
                if row:
                    cursor.execute("""
                        UPDATE uniqueemailsnotnullreciever SET sender = %s WHERE id = %s
                    """, (sender, row[0]))
                else:
                    cursor.execute("""
                        INSERT INTO uniqueemailsnotnullreciever (sender, receiver, date)
                        VALUES (%s, NULL, %s)
                    """, (sender, date))
                used_senders.add(sender)
                count += 1

            #  Case 3: New receiver only
            elif sender in used_senders and receiver not in used_receivers:
                cursor.execute("""
                    SELECT id FROM uniqueemailsnotnullreciever
                    WHERE sender = %s AND receiver IS NULL
                    LIMIT 1
                """, (sender,))
                row = cursor.fetchone()
                if row:
                    cursor.execute("""
                        UPDATE uniqueemailsnotnullreciever SET receiver = %s WHERE id = %s
                    """, (receiver, row[0]))
                else:
                    cursor.execute("""
                        INSERT INTO uniqueemailsnotnullreciever (sender, receiver, date)
                        VALUES (NULL, %s, %s)
                    """, (receiver, date))
                used_receivers.add(receiver)
                count += 1

            conn.commit()

        print(f"\n Total unique rows inserted/updated: {count}")

    except Error as e:
        print(f" Database error: {e}")

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals() and conn.is_connected():
            conn.close()
        print(" Done!")

if __name__ == "__main__":
    process_mbox()