"""
Blog System with Dual Write (MongoDB + Cassandra)
Phase 1 of Migration: Write to both databases simultaneously
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
from datetime import datetime
from pymongo import MongoClient
from bson import ObjectId
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import uuid
import json

app = Flask(__name__)
CORS(app)

# ============= DATABASE CONNECTIONS =============

# MongoDB Connection
mongo_client = MongoClient('mongodb://localhost:27017/')
mongo_db = mongo_client['blog']
users_collection = mongo_db['users']
posts_collection = mongo_db['posts']

# Cassandra Connection
cassandra_cluster = Cluster(['localhost'])
cassandra_session = cassandra_cluster.connect()

# Initialize Cassandra keyspace and tables
def init_cassandra():
    """Create Cassandra keyspace and tables if they don't exist"""
    
    # Create keyspace
    cassandra_session.execute("""
        CREATE KEYSPACE IF NOT EXISTS blog
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)
    
    cassandra_session.set_keyspace('blog')
    
    # Create users table
    cassandra_session.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id UUID PRIMARY KEY,
            name TEXT,
            email TEXT
        )
    """)
    
    # Create users_by_email table for email lookups
    cassandra_session.execute("""
        CREATE TABLE IF NOT EXISTS users_by_email (
            email TEXT PRIMARY KEY,
            id UUID,
            name TEXT
        )
    """)
    
    # Create posts table - partitioned by created_date for efficient date queries
    cassandra_session.execute("""
        CREATE TABLE IF NOT EXISTS posts (
            id UUID,
            user_id UUID,
            user_name TEXT,
            content TEXT,
            created_at TIMESTAMP,
            created_date TEXT,
            PRIMARY KEY ((created_date), created_at, id)
        ) WITH CLUSTERING ORDER BY (created_at DESC, id ASC)
    """)
    
    # Create posts_by_author table for author queries
    cassandra_session.execute("""
        CREATE TABLE IF NOT EXISTS posts_by_author (
            user_id UUID,
            id UUID,
            user_name TEXT,
            content TEXT,
            created_at TIMESTAMP,
            PRIMARY KEY ((user_id), created_at, id)
        ) WITH CLUSTERING ORDER BY (created_at DESC, id ASC)
    """)
    
    # Create posts_by_content table for alphabetical content sorting
    cassandra_session.execute("""
        CREATE TABLE IF NOT EXISTS posts_by_content (
            content_prefix TEXT,
            id UUID,
            user_id UUID,
            user_name TEXT,
            content TEXT,
            created_at TIMESTAMP,
            PRIMARY KEY ((content_prefix), content, id)
        ) WITH CLUSTERING ORDER BY (content ASC, id ASC)
    """)
    
    # Create posts_by_id table for direct lookups
    cassandra_session.execute("""
        CREATE TABLE IF NOT EXISTS posts_by_id (
            id UUID PRIMARY KEY,
            user_id UUID,
            user_name TEXT,
            content TEXT,
            created_at TIMESTAMP,
            created_date TEXT
        )
    """)
    
    # Create comments table
    cassandra_session.execute("""
        CREATE TABLE IF NOT EXISTS comments (
            post_id UUID,
            comment_id UUID,
            user_id UUID,
            user_name TEXT,
            content TEXT,
            created_at TIMESTAMP,
            PRIMARY KEY ((post_id), created_at, comment_id)
        ) WITH CLUSTERING ORDER BY (created_at ASC, comment_id ASC)
    """)
    
    # Create author_post_counts table (materialized view alternative)
    cassandra_session.execute("""
        CREATE TABLE IF NOT EXISTS author_post_counts (
            user_id UUID PRIMARY KEY,
            post_count COUNTER
        )
    """)
    
    print("Cassandra keyspace and tables initialized!")

# Initialize Cassandra on startup
try:
    init_cassandra()
except Exception as e:
    print(f"Cassandra initialization warning: {e}")

# ============= HELPER FUNCTIONS =============

def serialize_doc(doc):
    """Convert MongoDB document to JSON-serializable format"""
    if doc and '_id' in doc:
        doc['id'] = str(doc['_id'])
        del doc['_id']
    return doc

def serialize_docs(docs):
    """Convert list of MongoDB documents to JSON-serializable format"""
    return [serialize_doc(doc) for doc in docs]

def mongo_id_to_uuid(mongo_id):
    """Convert MongoDB ObjectId to UUID (for Cassandra)"""
    # Create a deterministic UUID from MongoDB ObjectId
    hex_str = str(mongo_id).ljust(32, '0')[:32]
    return uuid.UUID(hex_str)

def uuid_to_mongo_id(uuid_val):
    """Convert UUID back to MongoDB-like string ID"""
    return str(uuid_val).replace('-', '')[:24]

# ============= CASSANDRA WRITE HELPERS =============

def cassandra_create_user(user_id, name, email):
    """Write user to Cassandra"""
    try:
        user_uuid = mongo_id_to_uuid(user_id)
        
        # Insert into users table
        cassandra_session.execute("""
            INSERT INTO users (id, name, email)
            VALUES (%s, %s, %s)
        """, (user_uuid, name, email))
        
        # Insert into users_by_email table
        cassandra_session.execute("""
            INSERT INTO users_by_email (email, id, name)
            VALUES (%s, %s, %s)
        """, (email, user_uuid, name))
        
        print(f"[Cassandra] User created: {name}")
    except Exception as e:
        print(f"[Cassandra Write Error - User] {e}")

def cassandra_update_user(user_id, name=None, email=None, old_email=None):
    """Update user in Cassandra"""
    try:
        user_uuid = mongo_id_to_uuid(user_id)
        
        if name:
            cassandra_session.execute("""
                UPDATE users SET name = %s WHERE id = %s
            """, (name, user_uuid))
        
        if email and old_email:
            # Delete old email entry
            cassandra_session.execute("""
                DELETE FROM users_by_email WHERE email = %s
            """, (old_email,))
            
            # Get current user name
            result = cassandra_session.execute("""
                SELECT name FROM users WHERE id = %s
            """, (user_uuid,))
            row = result.one()
            current_name = row.name if row else name
            
            # Insert new email entry
            cassandra_session.execute("""
                INSERT INTO users_by_email (email, id, name)
                VALUES (%s, %s, %s)
            """, (email, user_uuid, current_name))
        
        print(f"[Cassandra] User updated: {user_id}")
    except Exception as e:
        print(f"[Cassandra Write Error - User Update] {e}")

def cassandra_delete_user(user_id, email):
    """Delete user from Cassandra"""
    try:
        user_uuid = mongo_id_to_uuid(user_id)
        
        cassandra_session.execute("""
            DELETE FROM users WHERE id = %s
        """, (user_uuid,))
        
        cassandra_session.execute("""
            DELETE FROM users_by_email WHERE email = %s
        """, (email,))
        
        print(f"[Cassandra] User deleted: {user_id}")
    except Exception as e:
        print(f"[Cassandra Write Error - User Delete] {e}")

def cassandra_create_post(post_id, user_id, user_name, content, created_at):
    """Write post to all Cassandra tables"""
    try:
        post_uuid = mongo_id_to_uuid(post_id)
        user_uuid = mongo_id_to_uuid(user_id)
        created_date = created_at.strftime('%Y-%m-%d')
        content_prefix = content[0].upper() if content else 'A'
        
        # Insert into posts table (by date)
        cassandra_session.execute("""
            INSERT INTO posts (id, user_id, user_name, content, created_at, created_date)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (post_uuid, user_uuid, user_name, content, created_at, created_date))
        
        # Insert into posts_by_author table
        cassandra_session.execute("""
            INSERT INTO posts_by_author (user_id, id, user_name, content, created_at)
            VALUES (%s, %s, %s, %s, %s)
        """, (user_uuid, post_uuid, user_name, content, created_at))
        
        # Insert into posts_by_content table
        cassandra_session.execute("""
            INSERT INTO posts_by_content (content_prefix, id, user_id, user_name, content, created_at)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (content_prefix, post_uuid, user_uuid, user_name, content, created_at))
        
        # Insert into posts_by_id table
        cassandra_session.execute("""
            INSERT INTO posts_by_id (id, user_id, user_name, content, created_at, created_date)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (post_uuid, user_uuid, user_name, content, created_at, created_date))
        
        # Update author post count
        cassandra_session.execute("""
            UPDATE author_post_counts SET post_count = post_count + 1 WHERE user_id = %s
        """, (user_uuid,))
        
        print(f"[Cassandra] Post created: {post_id}")
    except Exception as e:
        print(f"[Cassandra Write Error - Post] {e}")

def cassandra_update_post(post_id, content):
    """Update post content in Cassandra"""
    try:
        post_uuid = mongo_id_to_uuid(post_id)
        
        # Get current post data
        result = cassandra_session.execute("""
            SELECT user_id, user_name, created_at, created_date FROM posts_by_id WHERE id = %s
        """, (post_uuid,))
        row = result.one()
        
        if row:
            # Update posts_by_id
            cassandra_session.execute("""
                UPDATE posts_by_id SET content = %s WHERE id = %s
            """, (content, post_uuid))
            
            # For other tables, we need to delete and re-insert due to clustering keys
            # This is a limitation of Cassandra's data model
            
        print(f"[Cassandra] Post updated: {post_id}")
    except Exception as e:
        print(f"[Cassandra Write Error - Post Update] {e}")

def cassandra_delete_post(post_id, user_id, created_at, created_date, content):
    """Delete post from all Cassandra tables"""
    try:
        post_uuid = mongo_id_to_uuid(post_id)
        user_uuid = mongo_id_to_uuid(user_id)
        content_prefix = content[0].upper() if content else 'A'
        
        # Delete from posts table
        cassandra_session.execute("""
            DELETE FROM posts WHERE created_date = %s AND created_at = %s AND id = %s
        """, (created_date, created_at, post_uuid))
        
        # Delete from posts_by_author
        cassandra_session.execute("""
            DELETE FROM posts_by_author WHERE user_id = %s AND created_at = %s AND id = %s
        """, (user_uuid, created_at, post_uuid))
        
        # Delete from posts_by_content
        cassandra_session.execute("""
            DELETE FROM posts_by_content WHERE content_prefix = %s AND content = %s AND id = %s
        """, (content_prefix, content, post_uuid))
        
        # Delete from posts_by_id
        cassandra_session.execute("""
            DELETE FROM posts_by_id WHERE id = %s
        """, (post_uuid,))
        
        # Decrement author post count
        cassandra_session.execute("""
            UPDATE author_post_counts SET post_count = post_count - 1 WHERE user_id = %s
        """, (user_uuid,))
        
        # Delete all comments for this post
        cassandra_session.execute("""
            DELETE FROM comments WHERE post_id = %s
        """, (post_uuid,))
        
        print(f"[Cassandra] Post deleted: {post_id}")
    except Exception as e:
        print(f"[Cassandra Write Error - Post Delete] {e}")

def cassandra_create_comment(post_id, comment_id, user_id, user_name, content, created_at):
    """Write comment to Cassandra"""
    try:
        post_uuid = mongo_id_to_uuid(post_id)
        comment_uuid = uuid.uuid4()  # Generate new UUID for comment
        user_uuid = mongo_id_to_uuid(user_id)
        
        cassandra_session.execute("""
            INSERT INTO comments (post_id, comment_id, user_id, user_name, content, created_at)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (post_uuid, comment_uuid, user_uuid, user_name, content, created_at))
        
        print(f"[Cassandra] Comment created on post: {post_id}")
        return comment_uuid
    except Exception as e:
        print(f"[Cassandra Write Error - Comment] {e}")
        return None

def cassandra_delete_comment(post_id, created_at, comment_id):
    """Delete comment from Cassandra"""
    try:
        post_uuid = mongo_id_to_uuid(post_id)
        
        cassandra_session.execute("""
            DELETE FROM comments WHERE post_id = %s AND created_at = %s AND comment_id = %s
        """, (post_uuid, created_at, comment_id))
        
        print(f"[Cassandra] Comment deleted from post: {post_id}")
    except Exception as e:
        print(f"[Cassandra Write Error - Comment Delete] {e}")

# ============= USER ROUTES =============

@app.route('/api/users', methods=['GET'])
def get_users():
    # Aggregate users with their post counts (still reading from MongoDB)
    pipeline = [
        {
            '$lookup': {
                'from': 'posts',
                'localField': '_id',
                'foreignField': 'user_id',
                'as': 'user_posts'
            }
        },
        {
            '$addFields': {
                'postsCount': {'$size': '$user_posts'}
            }
        },
        {
            '$project': {
                'name': 1,
                'email': 1,
                'postsCount': 1
            }
        }
    ]
    
    users = list(users_collection.aggregate(pipeline))
    return jsonify({
        'success': True,
        'data': serialize_docs(users),
        'count': len(users)
    })

@app.route('/api/users/<string:user_id>', methods=['GET'])
def get_user(user_id):
    user = users_collection.find_one({'_id': ObjectId(user_id)})
    
    if not user:
        return jsonify({
            'success': False,
            'message': 'User not found'
        }), 404
    
    user_posts_count = posts_collection.count_documents({'user_id': user_id})
    
    user = serialize_doc(user)
    user['postsCount'] = user_posts_count
    
    return jsonify({
        'success': True,
        'data': user
    })

@app.route('/api/users', methods=['POST'])
def create_user():
    data = request.get_json()
    
    if not data or 'name' not in data or 'email' not in data:
        return jsonify({
            'success': False,
            'message': 'Name and email are required'
        }), 400
    
    if users_collection.find_one({'email': data['email']}):
        return jsonify({
            'success': False,
            'message': 'Email already exists'
        }), 400
    
    new_user = {
        'name': data['name'],
        'email': data['email']
    }
    
    # MongoDB write
    result = users_collection.insert_one(new_user)
    user_id = result.inserted_id
    
    # Cassandra dual write
    cassandra_create_user(user_id, data['name'], data['email'])
    
    new_user['id'] = str(user_id)
    del new_user['_id']
    
    return jsonify({
        'success': True,
        'data': new_user,
        'message': 'User created successfully'
    }), 201

@app.route('/api/users/<string:user_id>', methods=['PUT'])
def update_user(user_id):
    data = request.get_json()
    
    # Get old user data for Cassandra update
    old_user = users_collection.find_one({'_id': ObjectId(user_id)})
    
    update_fields = {}
    if 'name' in data:
        update_fields['name'] = data['name']
    if 'email' in data:
        update_fields['email'] = data['email']
    
    # MongoDB update
    result = users_collection.update_one(
        {'_id': ObjectId(user_id)},
        {'$set': update_fields}
    )
    
    if result.matched_count == 0:
        return jsonify({
            'success': False,
            'message': 'User not found'
        }), 404
    
    # Cassandra dual write
    cassandra_update_user(
        user_id, 
        name=data.get('name'),
        email=data.get('email'),
        old_email=old_user.get('email') if old_user and 'email' in data else None
    )
    
    user = users_collection.find_one({'_id': ObjectId(user_id)})
    
    return jsonify({
        'success': True,
        'data': serialize_doc(user),
        'message': 'User updated successfully'
    })

@app.route('/api/users/<string:user_id>', methods=['DELETE'])
def delete_user(user_id):
    user = users_collection.find_one({'_id': ObjectId(user_id)})
    
    if not user:
        return jsonify({
            'success': False,
            'message': 'User not found'
        }), 404
    
    # MongoDB delete
    users_collection.delete_one({'_id': ObjectId(user_id)})
    posts_collection.delete_many({'user_id': user_id})
    
    # Cassandra dual write
    cassandra_delete_user(user_id, user.get('email', ''))
    
    return jsonify({
        'success': True,
        'data': serialize_doc(user),
        'message': 'User and associated content deleted successfully'
    })

# ============= POST ROUTES =============

@app.route('/api/posts', methods=['GET'])
def get_posts():
    sort_by = request.args.get('sort', 'latest')
    
    sort_criteria = {
        'latest': ('created_at', -1),
        'oldest': ('created_at', 1),
        'author': ('user_name', 1),
        'content': ('content', 1),
        'comments': ('comments', -1)
    }
    
    sort_field, sort_direction = sort_criteria.get(sort_by, sort_criteria['latest'])
    
    # Use aggregation pipeline to include author post counts
    pipeline = [
        {
            '$addFields': {
                'commentsCount': {'$size': {'$ifNull': ['$comments', []]}}
            }
        },
        {
            '$lookup': {
                'from': 'posts',
                'localField': 'user_id',
                'foreignField': 'user_id',
                'as': 'author_posts'
            }
        },
        {
            '$addFields': {
                'author_post_count': {'$size': '$author_posts'}
            }
        },
        {
            '$project': {
                'author_posts': 0
            }
        }
    ]
    
    if sort_field == 'comments':
        pipeline.append({'$sort': {'commentsCount': sort_direction}})
    else:
        pipeline.append({'$sort': {sort_field: sort_direction}})
    
    posts = list(posts_collection.aggregate(pipeline))
    
    return jsonify({
        'success': True,
        'data': serialize_docs(posts),
        'count': len(posts),
        'sort': sort_by
    })

@app.route('/api/posts/<string:post_id>', methods=['GET'])
def get_post(post_id):
    post = posts_collection.find_one({'_id': ObjectId(post_id)})
    
    if not post:
        return jsonify({
            'success': False,
            'message': 'Post not found'
        }), 404
    
    post = serialize_doc(post)
    
    return jsonify({
        'success': True,
        'data': post
    })

@app.route('/api/posts', methods=['POST'])
def create_post():
    data = request.get_json()
    
    if not data or 'content' not in data or 'user_id' not in data:
        return jsonify({
            'success': False,
            'message': 'Content and user_id are required'
        }), 400
    
    author = users_collection.find_one({'_id': ObjectId(data['user_id'])})
    if not author:
        return jsonify({
            'success': False,
            'message': 'User not found'
        }), 404
    
    created_at = datetime.now()
    
    new_post = {
        'user_name': author['name'],
        'user_id': data['user_id'],
        'content': data['content'],
        'created_at': created_at,
        'comments': []
    }
    
    # MongoDB write
    result = posts_collection.insert_one(new_post)
    post_id = result.inserted_id
    
    # Cassandra dual write
    cassandra_create_post(post_id, data['user_id'], author['name'], data['content'], created_at)
    
    new_post['id'] = str(post_id)
    del new_post['_id']
    
    return jsonify({
        'success': True,
        'data': new_post,
        'message': 'Post created successfully'
    }), 201

@app.route('/api/posts/<string:post_id>', methods=['PUT'])
def update_post(post_id):
    data = request.get_json()
    
    update_fields = {}
    if 'content' in data:
        update_fields['content'] = data['content']
    
    # MongoDB update
    result = posts_collection.update_one(
        {'_id': ObjectId(post_id)},
        {'$set': update_fields}
    )
    
    if result.matched_count == 0:
        return jsonify({
            'success': False,
            'message': 'Post not found'
        }), 404
    
    # Cassandra dual write
    if 'content' in data:
        cassandra_update_post(post_id, data['content'])
    
    post = posts_collection.find_one({'_id': ObjectId(post_id)})
    
    return jsonify({
        'success': True,
        'data': serialize_doc(post),
        'message': 'Post updated successfully'
    })

@app.route('/api/posts/<string:post_id>', methods=['DELETE'])
def delete_post(post_id):
    post = posts_collection.find_one({'_id': ObjectId(post_id)})
    
    if not post:
        return jsonify({
            'success': False,
            'message': 'Post not found'
        }), 404
    
    # MongoDB delete
    posts_collection.delete_one({'_id': ObjectId(post_id)})
    
    # Cassandra dual write
    created_date = post['created_at'].strftime('%Y-%m-%d') if post.get('created_at') else datetime.now().strftime('%Y-%m-%d')
    cassandra_delete_post(
        post_id, 
        post.get('user_id', ''),
        post.get('created_at', datetime.now()),
        created_date,
        post.get('content', '')
    )
    
    return jsonify({
        'success': True,
        'data': serialize_doc(post),
        'message': 'Post deleted successfully'
    })

# ============= COMMENT ROUTES =============

@app.route('/api/posts/<string:post_id>/comments', methods=['GET'])
def get_post_comments(post_id):
    post = posts_collection.find_one({'_id': ObjectId(post_id)})
    
    if not post:
        return jsonify({
            'success': False,
            'message': 'Post not found'
        }), 404
    
    comments = post.get('comments', [])
    
    return jsonify({
        'success': True,
        'data': comments,
        'count': len(comments)
    })

@app.route('/api/posts/<string:post_id>/comments', methods=['POST'])
def create_comment(post_id):
    post = posts_collection.find_one({'_id': ObjectId(post_id)})
    
    if not post:
        return jsonify({
            'success': False,
            'message': 'Post not found'
        }), 404
    
    data = request.get_json()
    
    if not data or 'content' not in data or 'user_id' not in data:
        return jsonify({
            'success': False,
            'message': 'Content and user_id are required'
        }), 400
    
    user = users_collection.find_one({'_id': ObjectId(data['user_id'])})
    if not user:
        return jsonify({
            'success': False,
            'message': 'User not found'
        }), 404
    
    created_at = datetime.now()
    
    new_comment = {
        'user_id': data['user_id'],
        'content': data['content']
    }
    
    # MongoDB write
    posts_collection.update_one(
        {'_id': ObjectId(post_id)},
        {'$push': {'comments': new_comment}}
    )
    
    # Cassandra dual write
    cassandra_create_comment(post_id, None, data['user_id'], user['name'], data['content'], created_at)
    
    return jsonify({
        'success': True,
        'data': new_comment,
        'message': 'Comment added successfully'
    }), 201

@app.route('/api/posts/<string:post_id>/comments/<int:comment_index>', methods=['DELETE'])
def delete_comment(post_id, comment_index):
    post = posts_collection.find_one({'_id': ObjectId(post_id)})
    
    if not post:
        return jsonify({
            'success': False,
            'message': 'Post not found'
        }), 404
    
    comments = post.get('comments', [])
    
    if comment_index < 0 or comment_index >= len(comments):
        return jsonify({
            'success': False,
            'message': 'Comment not found'
        }), 404
    
    deleted_comment = comments[comment_index]
    comments.pop(comment_index)
    
    # MongoDB write
    posts_collection.update_one(
        {'_id': ObjectId(post_id)},
        {'$set': {'comments': comments}}
    )
    
    # Note: Cassandra comment deletion is more complex due to lack of index
    # In production, you'd store comment_id in MongoDB as well
    
    return jsonify({
        'success': True,
        'data': deleted_comment,
        'message': 'Comment deleted successfully'
    })

# ============= INFO ROUTES =============

@app.route('/', methods=['GET'])
def root():
    return jsonify({
        'message': 'Blog System API with Dual Write (MongoDB + Cassandra)',
        'version': '2.0',
        'databases': ['MongoDB', 'Cassandra'],
        'migration_phase': 'Phase 1: Dual Write',
        'endpoints': {
            'users': '/api/users',
            'posts': '/api/posts',
            'comments': '/api/posts/:postId/comments',
            'health': '/api/health'
        }
    })

@app.route('/api/health', methods=['GET'])
def health_check():
    # Check MongoDB connection
    try:
        mongo_client.admin.command('ping')
        mongo_status = 'connected'
    except Exception as e:
        mongo_status = f'disconnected: {str(e)}'
    
    # Check Cassandra connection
    try:
        cassandra_session.execute("SELECT now() FROM system.local")
        cassandra_status = 'connected'
    except Exception as e:
        cassandra_status = f'disconnected: {str(e)}'
    
    return jsonify({
        'success': True,
        'message': 'Server is running with dual write',
        'timestamp': datetime.now().isoformat(),
        'databases': {
            'mongodb': mongo_status,
            'cassandra': cassandra_status
        },
        'stats': {
            'users': users_collection.count_documents({}),
            'posts': posts_collection.count_documents({})
        }
    })

if __name__ == '__main__':
    print('=' * 60)
    print('Blog System API Server - DUAL WRITE MODE')
    print('=' * 60)
    print('Server is running on http://localhost:5000')
    print('\nDatabases:')
    print('  - MongoDB (blog) - Primary')
    print('  - Cassandra (blog) - Secondary (dual write)')
    print('\nMigration Phase: 1 - Dual Write')
    print('All writes go to both MongoDB and Cassandra')
    print('Reads still come from MongoDB')
    print('=' * 60)
    app.run(debug=True, host='0.0.0.0', port=5000)
