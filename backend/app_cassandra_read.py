"""
Blog System with Cassandra Reads (MongoDB fallback)
Phase 3 of Migration: Read from Cassandra, dual write continues
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
from datetime import datetime, timedelta
from pymongo import MongoClient
from bson import ObjectId
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import uuid

app = Flask(__name__)
CORS(app)

# ============= DATABASE CONNECTIONS =============

# MongoDB Connection (for writes and fallback)
mongo_client = MongoClient('mongodb://localhost:27017/')
mongo_db = mongo_client['blog']
users_collection = mongo_db['users']
posts_collection = mongo_db['posts']

# Cassandra Connection (primary for reads)
cassandra_cluster = Cluster(['localhost'])
cassandra_session = cassandra_cluster.connect('blog')

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
    hex_str = str(mongo_id).ljust(32, '0')[:32]
    return uuid.UUID(hex_str)

def uuid_to_string(uuid_val):
    """Convert UUID to string ID"""
    return str(uuid_val).replace('-', '')[:24]

def serialize_cassandra_row(row, include_comments_count=False):
    """Convert Cassandra row to JSON-serializable format"""
    data = {
        'id': uuid_to_string(row.id) if hasattr(row, 'id') else None,
        'user_id': uuid_to_string(row.user_id) if hasattr(row, 'user_id') else None,
        'user_name': row.user_name if hasattr(row, 'user_name') else None,
        'content': row.content if hasattr(row, 'content') else None,
        'created_at': row.created_at.isoformat() if hasattr(row, 'created_at') and row.created_at else None,
    }
    return data

# ============= CASSANDRA WRITE HELPERS (Same as dual write) =============

def cassandra_create_user(user_id, name, email):
    """Write user to Cassandra"""
    try:
        user_uuid = mongo_id_to_uuid(user_id)
        cassandra_session.execute("""
            INSERT INTO users (id, name, email)
            VALUES (%s, %s, %s)
        """, (user_uuid, name, email))
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
            cassandra_session.execute("DELETE FROM users_by_email WHERE email = %s", (old_email,))
            result = cassandra_session.execute("SELECT name FROM users WHERE id = %s", (user_uuid,))
            row = result.one()
            current_name = row.name if row else name
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
        cassandra_session.execute("DELETE FROM users WHERE id = %s", (user_uuid,))
        cassandra_session.execute("DELETE FROM users_by_email WHERE email = %s", (email,))
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
        
        cassandra_session.execute("""
            INSERT INTO posts (id, user_id, user_name, content, created_at, created_date)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (post_uuid, user_uuid, user_name, content, created_at, created_date))
        
        cassandra_session.execute("""
            INSERT INTO posts_by_author (user_id, id, user_name, content, created_at)
            VALUES (%s, %s, %s, %s, %s)
        """, (user_uuid, post_uuid, user_name, content, created_at))
        
        cassandra_session.execute("""
            INSERT INTO posts_by_content (content_prefix, id, user_id, user_name, content, created_at)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (content_prefix, post_uuid, user_uuid, user_name, content, created_at))
        
        cassandra_session.execute("""
            INSERT INTO posts_by_id (id, user_id, user_name, content, created_at, created_date)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (post_uuid, user_uuid, user_name, content, created_at, created_date))
        
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
        cassandra_session.execute("UPDATE posts_by_id SET content = %s WHERE id = %s", (content, post_uuid))
        print(f"[Cassandra] Post updated: {post_id}")
    except Exception as e:
        print(f"[Cassandra Write Error - Post Update] {e}")

def cassandra_delete_post(post_id, user_id, created_at, created_date, content):
    """Delete post from all Cassandra tables"""
    try:
        post_uuid = mongo_id_to_uuid(post_id)
        user_uuid = mongo_id_to_uuid(user_id)
        content_prefix = content[0].upper() if content else 'A'
        
        cassandra_session.execute("""
            DELETE FROM posts WHERE created_date = %s AND created_at = %s AND id = %s
        """, (created_date, created_at, post_uuid))
        
        cassandra_session.execute("""
            DELETE FROM posts_by_author WHERE user_id = %s AND created_at = %s AND id = %s
        """, (user_uuid, created_at, post_uuid))
        
        cassandra_session.execute("""
            DELETE FROM posts_by_content WHERE content_prefix = %s AND content = %s AND id = %s
        """, (content_prefix, content, post_uuid))
        
        cassandra_session.execute("DELETE FROM posts_by_id WHERE id = %s", (post_uuid,))
        
        cassandra_session.execute("""
            UPDATE author_post_counts SET post_count = post_count - 1 WHERE user_id = %s
        """, (user_uuid,))
        
        cassandra_session.execute("DELETE FROM comments WHERE post_id = %s", (post_uuid,))
        
        print(f"[Cassandra] Post deleted: {post_id}")
    except Exception as e:
        print(f"[Cassandra Write Error - Post Delete] {e}")

def cassandra_create_comment(post_id, user_id, user_name, content, created_at):
    """Write comment to Cassandra"""
    try:
        post_uuid = mongo_id_to_uuid(post_id)
        comment_uuid = uuid.uuid4()
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

# ============= CASSANDRA READ HELPERS =============

def get_author_post_count(user_uuid):
    """Get author's post count from Cassandra"""
    try:
        result = cassandra_session.execute("""
            SELECT post_count FROM author_post_counts WHERE user_id = %s
        """, (user_uuid,))
        row = result.one()
        return row.post_count if row else 0
    except:
        return 0

def get_comments_count(post_uuid):
    """Get comments count for a post from Cassandra"""
    try:
        result = cassandra_session.execute("""
            SELECT COUNT(*) as count FROM comments WHERE post_id = %s
        """, (post_uuid,))
        row = result.one()
        return row.count if row else 0
    except:
        return 0

def get_comments_for_post(post_uuid):
    """Get all comments for a post from Cassandra"""
    try:
        result = cassandra_session.execute("""
            SELECT comment_id, user_id, user_name, content, created_at 
            FROM comments WHERE post_id = %s
        """, (post_uuid,))
        comments = []
        for row in result:
            comments.append({
                'user_id': uuid_to_string(row.user_id),
                'user_name': row.user_name,
                'content': row.content,
                'created_at': row.created_at.isoformat() if row.created_at else None
            })
        return comments
    except Exception as e:
        print(f"Error getting comments: {e}")
        return []

# ============= USER ROUTES (Read from Cassandra) =============

@app.route('/api/users', methods=['GET'])
def get_users():
    """Get all users - READ FROM CASSANDRA"""
    try:
        result = cassandra_session.execute("SELECT id, name, email FROM users")
        users = []
        for row in result:
            post_count = get_author_post_count(row.id)
            users.append({
                'id': uuid_to_string(row.id),
                'name': row.name,
                'email': row.email,
                'postsCount': post_count
            })
        
        return jsonify({
            'success': True,
            'data': users,
            'count': len(users),
            'source': 'cassandra'
        })
    except Exception as e:
        print(f"Cassandra read error, falling back to MongoDB: {e}")
        # Fallback to MongoDB
        pipeline = [
            {'$lookup': {'from': 'posts', 'localField': '_id', 'foreignField': 'user_id', 'as': 'user_posts'}},
            {'$addFields': {'postsCount': {'$size': '$user_posts'}}},
            {'$project': {'name': 1, 'email': 1, 'postsCount': 1}}
        ]
        users = list(users_collection.aggregate(pipeline))
        return jsonify({
            'success': True,
            'data': serialize_docs(users),
            'count': len(users),
            'source': 'mongodb_fallback'
        })

@app.route('/api/users/<string:user_id>', methods=['GET'])
def get_user(user_id):
    """Get single user - READ FROM CASSANDRA"""
    try:
        user_uuid = mongo_id_to_uuid(user_id)
        result = cassandra_session.execute("""
            SELECT id, name, email FROM users WHERE id = %s
        """, (user_uuid,))
        row = result.one()
        
        if not row:
            return jsonify({'success': False, 'message': 'User not found'}), 404
        
        post_count = get_author_post_count(user_uuid)
        
        return jsonify({
            'success': True,
            'data': {
                'id': uuid_to_string(row.id),
                'name': row.name,
                'email': row.email,
                'postsCount': post_count
            },
            'source': 'cassandra'
        })
    except Exception as e:
        print(f"Cassandra read error, falling back to MongoDB: {e}")
        user = users_collection.find_one({'_id': ObjectId(user_id)})
        if not user:
            return jsonify({'success': False, 'message': 'User not found'}), 404
        user = serialize_doc(user)
        user['postsCount'] = posts_collection.count_documents({'user_id': user_id})
        return jsonify({'success': True, 'data': user, 'source': 'mongodb_fallback'})

@app.route('/api/users', methods=['POST'])
def create_user():
    data = request.get_json()
    
    if not data or 'name' not in data or 'email' not in data:
        return jsonify({'success': False, 'message': 'Name and email are required'}), 400
    
    if users_collection.find_one({'email': data['email']}):
        return jsonify({'success': False, 'message': 'Email already exists'}), 400
    
    new_user = {'name': data['name'], 'email': data['email']}
    result = users_collection.insert_one(new_user)
    user_id = result.inserted_id
    
    # Dual write
    cassandra_create_user(user_id, data['name'], data['email'])
    
    new_user['id'] = str(user_id)
    del new_user['_id']
    
    return jsonify({'success': True, 'data': new_user, 'message': 'User created successfully'}), 201

@app.route('/api/users/<string:user_id>', methods=['PUT'])
def update_user(user_id):
    data = request.get_json()
    old_user = users_collection.find_one({'_id': ObjectId(user_id)})
    
    update_fields = {}
    if 'name' in data:
        update_fields['name'] = data['name']
    if 'email' in data:
        update_fields['email'] = data['email']
    
    result = users_collection.update_one({'_id': ObjectId(user_id)}, {'$set': update_fields})
    
    if result.matched_count == 0:
        return jsonify({'success': False, 'message': 'User not found'}), 404
    
    # Dual write
    cassandra_update_user(user_id, name=data.get('name'), email=data.get('email'),
                          old_email=old_user.get('email') if old_user and 'email' in data else None)
    
    user = users_collection.find_one({'_id': ObjectId(user_id)})
    return jsonify({'success': True, 'data': serialize_doc(user), 'message': 'User updated successfully'})

@app.route('/api/users/<string:user_id>', methods=['DELETE'])
def delete_user(user_id):
    user = users_collection.find_one({'_id': ObjectId(user_id)})
    
    if not user:
        return jsonify({'success': False, 'message': 'User not found'}), 404
    
    users_collection.delete_one({'_id': ObjectId(user_id)})
    posts_collection.delete_many({'user_id': user_id})
    
    # Dual write
    cassandra_delete_user(user_id, user.get('email', ''))
    
    return jsonify({'success': True, 'data': serialize_doc(user), 'message': 'User deleted successfully'})

# ============= POST ROUTES (Read from Cassandra) =============

@app.route('/api/posts', methods=['GET'])
def get_posts():
    """Get all posts with sorting - READ FROM CASSANDRA"""
    sort_by = request.args.get('sort', 'latest')
    
    try:
        posts = []
        
        if sort_by in ['latest', 'oldest']:
            # Get posts from last 30 days (can be adjusted)
            today = datetime.now()
            dates = [(today - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(30)]
            
            all_posts = []
            for date in dates:
                result = cassandra_session.execute("""
                    SELECT id, user_id, user_name, content, created_at, created_date 
                    FROM posts WHERE created_date = %s
                """, (date,))
                all_posts.extend(list(result))
            
            # Sort by date
            all_posts.sort(key=lambda x: x.created_at if x.created_at else datetime.min, 
                          reverse=(sort_by == 'latest'))
            
            for row in all_posts:
                post_uuid = row.id
                user_uuid = row.user_id
                comments_count = get_comments_count(post_uuid)
                author_post_count = get_author_post_count(user_uuid)
                comments = get_comments_for_post(post_uuid)
                
                posts.append({
                    'id': uuid_to_string(row.id),
                    'user_id': uuid_to_string(row.user_id),
                    'user_name': row.user_name,
                    'content': row.content,
                    'created_at': row.created_at.isoformat() if row.created_at else None,
                    'commentsCount': comments_count,
                    'author_post_count': author_post_count,
                    'comments': comments
                })
        
        elif sort_by == 'content':
            # Get posts sorted by content alphabetically
            all_posts = []
            for prefix in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ':
                result = cassandra_session.execute("""
                    SELECT id, user_id, user_name, content, created_at 
                    FROM posts_by_content WHERE content_prefix = %s
                """, (prefix,))
                all_posts.extend(list(result))
            
            for row in all_posts:
                post_uuid = row.id
                user_uuid = row.user_id
                comments_count = get_comments_count(post_uuid)
                author_post_count = get_author_post_count(user_uuid)
                comments = get_comments_for_post(post_uuid)
                
                posts.append({
                    'id': uuid_to_string(row.id),
                    'user_id': uuid_to_string(row.user_id),
                    'user_name': row.user_name,
                    'content': row.content,
                    'created_at': row.created_at.isoformat() if row.created_at else None,
                    'commentsCount': comments_count,
                    'author_post_count': author_post_count,
                    'comments': comments
                })
        
        elif sort_by == 'author':
            # Get all users and their posts
            users_result = cassandra_session.execute("SELECT id, name FROM users")
            all_posts = []
            
            for user_row in users_result:
                result = cassandra_session.execute("""
                    SELECT id, user_id, user_name, content, created_at 
                    FROM posts_by_author WHERE user_id = %s
                """, (user_row.id,))
                all_posts.extend(list(result))
            
            # Sort by author name
            all_posts.sort(key=lambda x: x.user_name.lower() if x.user_name else '')
            
            for row in all_posts:
                post_uuid = row.id
                user_uuid = row.user_id
                comments_count = get_comments_count(post_uuid)
                author_post_count = get_author_post_count(user_uuid)
                comments = get_comments_for_post(post_uuid)
                
                posts.append({
                    'id': uuid_to_string(row.id),
                    'user_id': uuid_to_string(row.user_id),
                    'user_name': row.user_name,
                    'content': row.content,
                    'created_at': row.created_at.isoformat() if row.created_at else None,
                    'commentsCount': comments_count,
                    'author_post_count': author_post_count,
                    'comments': comments
                })
        
        else:
            # Default: get from posts_by_id
            result = cassandra_session.execute("SELECT id, user_id, user_name, content, created_at FROM posts_by_id")
            for row in result:
                post_uuid = row.id
                user_uuid = row.user_id
                comments_count = get_comments_count(post_uuid)
                author_post_count = get_author_post_count(user_uuid)
                comments = get_comments_for_post(post_uuid)
                
                posts.append({
                    'id': uuid_to_string(row.id),
                    'user_id': uuid_to_string(row.user_id),
                    'user_name': row.user_name,
                    'content': row.content,
                    'created_at': row.created_at.isoformat() if row.created_at else None,
                    'commentsCount': comments_count,
                    'author_post_count': author_post_count,
                    'comments': comments
                })
        
        return jsonify({
            'success': True,
            'data': posts,
            'count': len(posts),
            'sort': sort_by,
            'source': 'cassandra'
        })
    
    except Exception as e:
        print(f"Cassandra read error, falling back to MongoDB: {e}")
        # Fallback to MongoDB (same as before)
        sort_criteria = {
            'latest': ('created_at', -1),
            'oldest': ('created_at', 1),
            'author': ('user_name', 1),
            'content': ('content', 1),
            'comments': ('comments', -1)
        }
        sort_field, sort_direction = sort_criteria.get(sort_by, sort_criteria['latest'])
        
        pipeline = [
            {'$addFields': {'commentsCount': {'$size': {'$ifNull': ['$comments', []]}}}},
            {'$lookup': {'from': 'posts', 'localField': 'user_id', 'foreignField': 'user_id', 'as': 'author_posts'}},
            {'$addFields': {'author_post_count': {'$size': '$author_posts'}}},
            {'$project': {'author_posts': 0}}
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
            'sort': sort_by,
            'source': 'mongodb_fallback'
        })

@app.route('/api/posts/<string:post_id>', methods=['GET'])
def get_post(post_id):
    """Get single post - READ FROM CASSANDRA"""
    try:
        post_uuid = mongo_id_to_uuid(post_id)
        result = cassandra_session.execute("""
            SELECT id, user_id, user_name, content, created_at, created_date 
            FROM posts_by_id WHERE id = %s
        """, (post_uuid,))
        row = result.one()
        
        if not row:
            return jsonify({'success': False, 'message': 'Post not found'}), 404
        
        comments = get_comments_for_post(post_uuid)
        
        return jsonify({
            'success': True,
            'data': {
                'id': uuid_to_string(row.id),
                'user_id': uuid_to_string(row.user_id),
                'user_name': row.user_name,
                'content': row.content,
                'created_at': row.created_at.isoformat() if row.created_at else None,
                'comments': comments
            },
            'source': 'cassandra'
        })
    except Exception as e:
        print(f"Cassandra read error, falling back to MongoDB: {e}")
        post = posts_collection.find_one({'_id': ObjectId(post_id)})
        if not post:
            return jsonify({'success': False, 'message': 'Post not found'}), 404
        return jsonify({'success': True, 'data': serialize_doc(post), 'source': 'mongodb_fallback'})

@app.route('/api/posts', methods=['POST'])
def create_post():
    data = request.get_json()
    
    if not data or 'content' not in data or 'user_id' not in data:
        return jsonify({'success': False, 'message': 'Content and user_id are required'}), 400
    
    author = users_collection.find_one({'_id': ObjectId(data['user_id'])})
    if not author:
        return jsonify({'success': False, 'message': 'User not found'}), 404
    
    created_at = datetime.now()
    new_post = {
        'user_name': author['name'],
        'user_id': data['user_id'],
        'content': data['content'],
        'created_at': created_at,
        'comments': []
    }
    
    result = posts_collection.insert_one(new_post)
    post_id = result.inserted_id
    
    # Dual write
    cassandra_create_post(post_id, data['user_id'], author['name'], data['content'], created_at)
    
    new_post['id'] = str(post_id)
    del new_post['_id']
    
    return jsonify({'success': True, 'data': new_post, 'message': 'Post created successfully'}), 201

@app.route('/api/posts/<string:post_id>', methods=['PUT'])
def update_post(post_id):
    data = request.get_json()
    
    update_fields = {}
    if 'content' in data:
        update_fields['content'] = data['content']
    
    result = posts_collection.update_one({'_id': ObjectId(post_id)}, {'$set': update_fields})
    
    if result.matched_count == 0:
        return jsonify({'success': False, 'message': 'Post not found'}), 404
    
    # Dual write
    if 'content' in data:
        cassandra_update_post(post_id, data['content'])
    
    post = posts_collection.find_one({'_id': ObjectId(post_id)})
    return jsonify({'success': True, 'data': serialize_doc(post), 'message': 'Post updated successfully'})

@app.route('/api/posts/<string:post_id>', methods=['DELETE'])
def delete_post(post_id):
    post = posts_collection.find_one({'_id': ObjectId(post_id)})
    
    if not post:
        return jsonify({'success': False, 'message': 'Post not found'}), 404
    
    posts_collection.delete_one({'_id': ObjectId(post_id)})
    
    # Dual write
    created_date = post['created_at'].strftime('%Y-%m-%d') if post.get('created_at') else datetime.now().strftime('%Y-%m-%d')
    cassandra_delete_post(post_id, post.get('user_id', ''), post.get('created_at', datetime.now()),
                          created_date, post.get('content', ''))
    
    return jsonify({'success': True, 'data': serialize_doc(post), 'message': 'Post deleted successfully'})

# ============= COMMENT ROUTES =============

@app.route('/api/posts/<string:post_id>/comments', methods=['GET'])
def get_post_comments(post_id):
    """Get comments for a post - READ FROM CASSANDRA"""
    try:
        post_uuid = mongo_id_to_uuid(post_id)
        comments = get_comments_for_post(post_uuid)
        
        return jsonify({
            'success': True,
            'data': comments,
            'count': len(comments),
            'source': 'cassandra'
        })
    except Exception as e:
        print(f"Cassandra read error, falling back to MongoDB: {e}")
        post = posts_collection.find_one({'_id': ObjectId(post_id)})
        if not post:
            return jsonify({'success': False, 'message': 'Post not found'}), 404
        comments = post.get('comments', [])
        return jsonify({'success': True, 'data': comments, 'count': len(comments), 'source': 'mongodb_fallback'})

@app.route('/api/posts/<string:post_id>/comments', methods=['POST'])
def create_comment(post_id):
    post = posts_collection.find_one({'_id': ObjectId(post_id)})
    
    if not post:
        return jsonify({'success': False, 'message': 'Post not found'}), 404
    
    data = request.get_json()
    
    if not data or 'content' not in data or 'user_id' not in data:
        return jsonify({'success': False, 'message': 'Content and user_id are required'}), 400
    
    user = users_collection.find_one({'_id': ObjectId(data['user_id'])})
    if not user:
        return jsonify({'success': False, 'message': 'User not found'}), 404
    
    created_at = datetime.now()
    new_comment = {'user_id': data['user_id'], 'content': data['content']}
    
    posts_collection.update_one({'_id': ObjectId(post_id)}, {'$push': {'comments': new_comment}})
    
    # Dual write
    cassandra_create_comment(post_id, data['user_id'], user['name'], data['content'], created_at)
    
    return jsonify({'success': True, 'data': new_comment, 'message': 'Comment added successfully'}), 201

@app.route('/api/posts/<string:post_id>/comments/<int:comment_index>', methods=['DELETE'])
def delete_comment(post_id, comment_index):
    post = posts_collection.find_one({'_id': ObjectId(post_id)})
    
    if not post:
        return jsonify({'success': False, 'message': 'Post not found'}), 404
    
    comments = post.get('comments', [])
    
    if comment_index < 0 or comment_index >= len(comments):
        return jsonify({'success': False, 'message': 'Comment not found'}), 404
    
    deleted_comment = comments[comment_index]
    comments.pop(comment_index)
    
    posts_collection.update_one({'_id': ObjectId(post_id)}, {'$set': {'comments': comments}})
    
    return jsonify({'success': True, 'data': deleted_comment, 'message': 'Comment deleted successfully'})

# ============= INFO ROUTES =============

@app.route('/', methods=['GET'])
def root():
    return jsonify({
        'message': 'Blog System API - Cassandra Reads with MongoDB Fallback',
        'version': '3.0',
        'databases': ['Cassandra (primary reads)', 'MongoDB (writes + fallback)'],
        'migration_phase': 'Phase 3: Read Migration',
        'endpoints': {
            'users': '/api/users',
            'posts': '/api/posts',
            'comments': '/api/posts/:postId/comments',
            'health': '/api/health'
        }
    })

@app.route('/api/health', methods=['GET'])
def health_check():
    try:
        mongo_client.admin.command('ping')
        mongo_status = 'connected'
    except Exception as e:
        mongo_status = f'disconnected: {str(e)}'
    
    try:
        cassandra_session.execute("SELECT now() FROM system.local")
        cassandra_status = 'connected'
    except Exception as e:
        cassandra_status = f'disconnected: {str(e)}'
    
    return jsonify({
        'success': True,
        'message': 'Server is running - Cassandra reads enabled',
        'timestamp': datetime.now().isoformat(),
        'databases': {
            'mongodb': mongo_status,
            'cassandra': cassandra_status
        }
    })

if __name__ == '__main__':
    print('=' * 60)
    print('Blog System API Server - CASSANDRA READ MODE')
    print('=' * 60)
    print('Server is running on http://localhost:5000')
    print('\nDatabases:')
    print('  - Cassandra (blog) - Primary for READS')
    print('  - MongoDB (blog) - Writes + Fallback')
    print('\nMigration Phase: 3 - Read Migration')
    print('Reads come from Cassandra (with MongoDB fallback)')
    print('Writes still go to both databases')
    print('=' * 60)
    app.run(debug=True, host='0.0.0.0', port=5000)
