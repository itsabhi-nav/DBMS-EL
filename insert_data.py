import datetime
from pymongo import MongoClient

def insert_data():
    # Connect to MongoDB
    client = MongoClient('mongodb://localhost:27017/')
    db = client['dbms']  # Replace 'your_database_name' with your actual database name
    collection = db['bday']  # Replace 'your_collection_name' with your actual collection name

    # Data to insert
    data = [
        {
            "Name": "Alice",
            "Birthday": datetime.datetime(1985, 3, 7),  # Example birthday
            "Year": "1985",
            "Email": "alice@example.com",
            "Dialogue": "Happy Birthday Alice!"
        },
        {
            "Name": "Bob",
            "Birthday": datetime.datetime(1992, 4, 7),  # Example birthday
            "Year": "1992",
            "Email": "chtgpt979@gmail.com",
            "Dialogue": "Happy Birthday Bob!"
        },
        {
            "Name": "Abhi",
            "Birthday": "1990-03-07",  # Example birthday as string
            "Year": "1990",
            "Email": "abhinavkumard.ec21@rvce.edu.in",
            "Dialogue": "Happy Birthday Charlie!"
        }
        # Add more documents as needed
    ]

    for doc in data:
        # Check if data already exists
        existing_data = collection.find_one({"Name": doc["Name"], "Email": doc["Email"]})
        if existing_data:
            print(f"Data for {doc['Name']} with email {doc['Email']} already exists in the database. Skipping insertion.")
        else:
            collection.insert_one(doc)
            print(f"Inserted data for {doc['Name']} with email {doc['Email']}.")

    client.close()  # Close the MongoDB connection

if __name__ == "__main__":
    insert_data()
