# MongoDB Integration Guide

This guide explains how to convert from dummy data (Python lists) to MongoDB.

## Table of Contents
1. [Prerequisites](#prerequisites)
2. [Key Differences](#key-differences)
3. [Installation Steps](#installation-steps)
4. [Understanding the Changes](#understanding-the-changes)
5. [Running with MongoDB](#running-with-mongodb)

---

## Prerequisites

### Install MongoDB
1. Download MongoDB Community Server from: https://www.mongodb.com/try/download/community
2. Install MongoDB on your system
3. Start MongoDB service

### Install Python MongoDB Driver
```powershell
cd C:\Users\pc\block-system\backend
pip install pymongo
```

---

## Key Differences

### Original (app.py) - Using Python Lists
```python
# Data stored in memory (lost when server stops)
users = [
    {
        'id': '1',
        'username': 'johndoe',
        'email': 'john@example.com',
        ...
    }
]

# Find a user
user = next((u for u in users if u['id'] == user_id), None)

# Add a user
users.append(new_user)

# Delete a user
users = [u for u in users if u['id'] != user_id]
```

### MongoDB Version (app_mongodb.py) - Using Database
```python
# Data stored in MongoDB (persists even after server stops)
from pymongo import MongoClient
from bson import ObjectId

client = MongoClient('mongodb://localhost:27017/')
db = client['blog']
users_collection = db['users']

# Find a user
user = users_collection.find_one({'_id': ObjectId(user_id)})

# Add a user
result = users_collection.insert_one(new_user)

# Delete a user
users_collection.delete_one({'_id': ObjectId(user_id)})
```

---

## Understanding the Changes

### 1. **Connection Setup**
```python
# Connect to MongoDB server
client = MongoClient('mongodb://localhost:27017/')

# Select database (creates it if doesn't exist)
db = client['blog']

# Select collections (like tables)
users_collection = db['users']
posts_collection = db['posts']
comments_collection = db['comments']
```

**What this means:**
- `MongoClient`: Connects to MongoDB server
- `blog`: Database name (you can change this)
- `users_collection`: Collection for storing users (like a table)

---

### 2. **MongoDB IDs vs Custom IDs**

**Dummy Data:**
```python
users = [
    {'id': '1', 'username': 'johndoe'}  # Manual ID
]
```

**MongoDB:**
```python
# MongoDB automatically creates '_id' field
{
    '_id': ObjectId('507f1f77bcf86cd799439011'),  # Automatic MongoDB ID
    'username': 'johndoe'
}
```

**Converting for API responses:**
```python
def serialize_doc(doc):
    """Convert _id to id for JSON response"""
    if doc and '_id' in doc:
        doc['id'] = str(doc['_id'])  # Convert ObjectId to string
        del doc['_id']               # Remove _id field
    return doc
```

---

### 3. **CRUD Operations Comparison**

#### **CREATE (Insert)**

**Dummy Data:**
```python
new_user = {'id': '123', 'username': 'newuser'}
users.append(new_user)
```

**MongoDB:**
```python
new_user = {'username': 'newuser'}  # No need for manual ID
result = users_collection.insert_one(new_user)
user_id = str(result.inserted_id)   # Get auto-generated ID
```

---

#### **READ (Find)**

**Dummy Data:**
```python
# Find all users
all_users = users

# Find one user
user = next((u for u in users if u['id'] == user_id), None)

# Find with condition
johns = [u for u in users if u['username'] == 'john']
```

**MongoDB:**
```python
# Find all users
all_users = list(users_collection.find({}))

# Find one user
user = users_collection.find_one({'_id': ObjectId(user_id)})

# Find with condition
johns = list(users_collection.find({'username': 'john'}))

# Sort results
sorted_posts = list(posts_collection.find({}).sort('createdAt', -1))
```

---

#### **UPDATE (Modify)**

**Dummy Data:**
```python
user = next((u for u in users if u['id'] == user_id), None)
if user:
    user['name'] = 'New Name'
    user['email'] = 'new@email.com'
```

**MongoDB:**
```python
users_collection.update_one(
    {'_id': ObjectId(user_id)},           # Filter: which document
    {'$set': {'name': 'New Name',         # Update: what to change
              'email': 'new@email.com'}}
)
```

**MongoDB Update Operators:**
- `$set`: Set field value
- `$unset`: Remove field
- `$inc`: Increment number
- `$push`: Add to array

---

#### **DELETE (Remove)**

**Dummy Data:**
```python
users = [u for u in users if u['id'] != user_id]
```

**MongoDB:**
```python
# Delete one document
users_collection.delete_one({'_id': ObjectId(user_id)})

# Delete many documents
posts_collection.delete_many({'authorId': user_id})
```

---

### 4. **Counting Documents**

**Dummy Data:**
```python
user_posts_count = len([p for p in posts if p['authorId'] == user_id])
```

**MongoDB:**
```python
user_posts_count = posts_collection.count_documents({'authorId': user_id})
```

---

### 5. **Checking Existence**

**Dummy Data:**
```python
exists = any(u['username'] == 'johndoe' for u in users)
```

**MongoDB:**
```python
exists = users_collection.find_one({'username': 'johndoe'}) is not None
# OR
exists = users_collection.count_documents({'username': 'johndoe'}) > 0
```

---

## MongoDB Query Examples

### Find Operations
```python
# Find all
users_collection.find({})

# Find with filter
users_collection.find({'name': 'John Doe'})

# Find with multiple conditions
posts_collection.find({'authorId': '123', 'title': 'Hello'})

# Find one
users_collection.find_one({'email': 'john@example.com'})
```

### Sort and Limit
```python
# Sort descending (-1) or ascending (1)
posts_collection.find({}).sort('createdAt', -1)

# Limit results
posts_collection.find({}).limit(10)

# Skip and limit (pagination)
posts_collection.find({}).skip(20).limit(10)
```

### Projection (Select specific fields)
```python
# Only return username and email
users_collection.find({}, {'username': 1, 'email': 1, '_id': 0})
```

---

## Running with MongoDB

### Step 1: Install MongoDB
Download and install from: https://www.mongodb.com/try/download/community

### Step 2: Start MongoDB
```powershell
# Windows (if installed as service, it starts automatically)
# Or manually start:
mongod
```

### Step 3: Install Python Package
```powershell
cd C:\Users\pc\block-system\backend
pip install pymongo
```

### Step 4: Initialize Database (First Time Only)
```python
# In app_mongodb.py, uncomment this line:
init_database()

# Run once to create dummy data:
python app_mongodb.py

# Then comment it out again to avoid duplicates
```

### Step 5: Run Server
```powershell
python app_mongodb.py
```

---

## Connection String Formats

### Local MongoDB
```python
MongoClient('mongodb://localhost:27017/')
```

### MongoDB with Authentication
```python
MongoClient('mongodb://username:password@localhost:27017/')
```

### MongoDB Atlas (Cloud)
```python
MongoClient('mongodb+srv://username:password@cluster.mongodb.net/dbname')
```

### With Options
```python
MongoClient('mongodb://localhost:27017/',
           serverSelectionTimeoutMS=5000,
           maxPoolSize=50)
```

---

## Debugging Tips

### Check if MongoDB is Running
```powershell
# Test connection
python -c "from pymongo import MongoClient; client = MongoClient('mongodb://localhost:27017/'); print('Connected!')"
```

### View Data in MongoDB
```powershell
# Open MongoDB shell
mongosh

# Use your database
use blog

# Show collections
show collections

# View data
db.users.find()
db.posts.find()
db.comments.find()
```

### Common Errors

1. **"No module named 'pymongo'"**
   - Solution: `pip install pymongo`

2. **"ServerSelectionTimeoutError"**
   - Solution: Make sure MongoDB is running

3. **"InvalidId" error**
   - Solution: Make sure you're converting string to ObjectId
   - `ObjectId(user_id)` not just `user_id`

---

## Summary of Changes

| Operation | Dummy Data | MongoDB |
|-----------|------------|---------|
| **Setup** | `users = []` | `users_collection = db['users']` |
| **Create** | `users.append(user)` | `users_collection.insert_one(user)` |
| **Read All** | `users` | `users_collection.find({})` |
| **Read One** | `next((u for u in users if u['id'] == id), None)` | `users_collection.find_one({'_id': ObjectId(id)})` |
| **Update** | `user['name'] = 'New'` | `users_collection.update_one({'_id': id}, {'$set': {'name': 'New'}})` |
| **Delete** | `users = [u for u in users if u['id'] != id]` | `users_collection.delete_one({'_id': ObjectId(id)})` |
| **Count** | `len(users)` | `users_collection.count_documents({})` |
| **Filter** | `[u for u in users if condition]` | `users_collection.find({'field': value})` |

---

## Next Steps for You

1. **Install MongoDB** on your computer
2. **Install pymongo**: `pip install pymongo`
3. **Compare** `app.py` and `app_mongodb.py` side by side
4. **Uncomment** `init_database()` line in `app_mongodb.py`
5. **Run** `python app_mongodb.py` once to create data
6. **Comment out** `init_database()` again
7. **Test** the API endpoints

The frontend code remains the same - it doesn't know if you're using dummy data or MongoDB!
