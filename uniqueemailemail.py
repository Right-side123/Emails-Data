import os
import mailbox
import mysql.connector
from mysql.connector import Error
from email.utils import parsedate_to_datetime, parseaddr

DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'rightside@123',
    'database': 'emailsdata'
}

MBOX_PATH = 'D:/Download/All mail Including Spam and Trash-002 (1).mbox'

def extract_emails(field):
    emails = []
    if field:
        for item in field.split(','):
            name, email = parseaddr(item.strip())
            if email:
                emails.append(email.lower())
    return emails

def process_mbox():
    if not os.path.exists(MBOX_PATH):
        print(f"MBOX file not found at: {MBOX_PATH}")
        return

    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        used_senders = set()
        used_receivers = set()

        # Load existing senders and receivers
        cursor.execute("SELECT sender, receiver FROM uniqueemails")
        for sender, receiver in cursor.fetchall():
            if sender:
                used_senders.add(sender.strip().lower())
            if receiver:
                used_receivers.add(receiver.strip().lower())

        mbox = mailbox.mbox(MBOX_PATH)
        count = 0

        for message in mbox:
            sender_list = extract_emails(message.get('From', ''))
            receiver_list = extract_emails(message.get('To', ''))
            raw_date = message.get('Date', '')

            try:
                parsed_date = parsedate_to_datetime(raw_date)
                date = parsed_date.strftime('%Y-%m-%d %H:%M:%S')
            except:
                date = None

            sender = sender_list[0] if sender_list else None

            for receiver in receiver_list:
                if not sender and not receiver:
                    continue

                sender = sender.lower() if sender else None
                receiver = receiver.lower() if receiver else None

                if sender not in used_senders and receiver not in used_receivers:
                    # ✅ Both new → insert both
                    cursor.execute("""
                        INSERT INTO uniqueemails (sender, receiver, date)
                        VALUES (%s, %s, %s)
                    """, (sender, receiver, date))
                    used_senders.add(sender)
                    used_receivers.add(receiver)
                    count += 1

                elif sender not in used_senders and receiver in used_receivers:
                    # ✅ Sender new, receiver old → insert sender only
                    cursor.execute("""
                        INSERT INTO uniqueemails (sender, receiver, date)
                        VALUES (%s, NULL, %s)
                    """, (sender, date))
                    used_senders.add(sender)
                    count += 1

                elif sender in used_senders and receiver not in used_receivers:
                    # ✅ Receiver new, sender old → update sender row
                    cursor.execute("""
                        UPDATE uniqueemails
                        SET receiver = %s
                        WHERE sender = %s AND receiver IS NULL
                        LIMIT 1
                    """, (receiver, sender))
                    used_receivers.add(receiver)
                    conn.commit()

                elif sender not in used_senders and not receiver:
                    # ✅ Sender new only
                    cursor.execute("""
                        INSERT INTO uniqueemails (sender, receiver, date)
                        VALUES (%s, NULL, %s)
                    """, (sender, date))
                    used_senders.add(sender)
                    count += 1

                elif sender in used_senders and not receiver:
                    continue  # already handled

                elif receiver not in used_receivers and not sender:
                    # ✅ Receiver new only
                    cursor.execute("""
                        UPDATE uniqueemails
                        SET sender = %s
                        WHERE receiver IS NULL
                        LIMIT 1
                    """, (receiver,))
                    used_receivers.add(receiver)
                    conn.commit()

        conn.commit()
        print(f"Total inserted/updated rows: {count}")

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