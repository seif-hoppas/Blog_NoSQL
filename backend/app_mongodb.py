from flask import Flask, jsonify, request
from flask_cors import CORS
from datetime import datetime
from pymongo import MongoClient
from bson import ObjectId

app = Flask(__name__)
CORS(app)

# MongoDB Connection
# Replace 'localhost' with your MongoDB server address if different
# Replace '27017' with your MongoDB port if different
client = MongoClient('mongodb://localhost:27017/')

# Database
db = client['blog']  # This creates/connects to a database named 'blog'

# Collections (like tables in SQL)
# In this structure, comments are embedded within posts
users_collection = db['users']
posts_collection = db['posts']

# Helper function to convert MongoDB ObjectId to string
def serialize_doc(doc):
    """Convert MongoDB document to JSON-serializable format"""
    if doc and '_id' in doc:
        doc['id'] = str(doc['_id'])
        del doc['_id']
    return doc

def serialize_docs(docs):
    """Convert list of MongoDB documents to JSON-serializable format"""
    return [serialize_doc(doc) for doc in docs]

# Optional: Initialize database with dummy data (run once)
def init_database():
    """
    This function initializes the database with dummy data.
    Run this once when you first set up MongoDB.
    Comment it out after first run or it will create duplicates.
    """
    # Clear existing data (optional - only for testing)
    users_collection.delete_many({})
    posts_collection.delete_many({})
    
    # Insert users with name, email, and _id
    user1_id = ObjectId()
    user2_id = ObjectId()
    
    users_collection.insert_one({
        '_id': user1_id,
        'name': 'mohamed magdy',
        'email': 'mohamed@mail.com'
    })
    
    users_collection.insert_one({
        '_id': user2_id,
        'name': 'joe',
        'email': 'joe@mail.com'
    })
    
    # Insert posts with embedded comments
    posts_collection.insert_one({
        'user_name': 'joe',
        'user_id': str(user2_id),
        'content': 'Am b7b el iti gednnnn',
        'created_at': datetime.now(),
        'comments': [
            {
                'user_id': str(user1_id),
                'content': 'fash55 ystaaaaaa'
            }
        ]
    })
    
    posts_collection.insert_one({
        'user_name': 'mohamed magdy',
        'user_id': str(user1_id),
        'content': 'Another post content here',
        'created_at': datetime.now(),
        'comments': []
    })
    
    print("Database initialized with dummy data!")

# Uncomment the line below to initialize the database with dummy data
# init_database()

# ============= USER ROUTES =============

# GET all users
@app.route('/api/users', methods=['GET'])
def get_users():
    # Aggregate users with their post counts in one query
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

# GET single user by ID
@app.route('/api/users/<string:user_id>', methods=['GET'])
def get_user(user_id):
    # Find user by ObjectId
    user = users_collection.find_one({'_id': ObjectId(user_id)})
    
    if not user:
        return jsonify({
            'success': False,
            'message': 'User not found'
        }), 404
    
    # Count user's posts
    user_posts_count = posts_collection.count_documents({'user_id': user_id})
    
    user = serialize_doc(user)
    user['postsCount'] = user_posts_count
    
    return jsonify({
        'success': True,
        'data': user
    })

# POST create new user
@app.route('/api/users', methods=['POST'])
def create_user():
    data = request.get_json()
    
    if not data or 'name' not in data or 'email' not in data:
        return jsonify({
            'success': False,
            'message': 'Name and email are required'
        }), 400
    
    # Check if email already exists
    if users_collection.find_one({'email': data['email']}):
        return jsonify({
            'success': False,
            'message': 'Email already exists'
        }), 400
    
    # Create new user document
    new_user = {
        'name': data['name'],
        'email': data['email']
    }
    
    # Insert into MongoDB
    result = users_collection.insert_one(new_user)
    new_user['id'] = str(result.inserted_id)
    del new_user['_id']
    
    return jsonify({
        'success': True,
        'data': new_user,
        'message': 'User created successfully'
    }), 201

# PUT update user
@app.route('/api/users/<string:user_id>', methods=['PUT'])
def update_user(user_id):
    data = request.get_json()
    
    # Prepare update fields
    update_fields = {}
    if 'name' in data:
        update_fields['name'] = data['name']
    if 'email' in data:
        update_fields['email'] = data['email']
    
    # Update in MongoDB
    result = users_collection.update_one(
        {'_id': ObjectId(user_id)},
        {'$set': update_fields}
    )
    
    if result.matched_count == 0:
        return jsonify({
            'success': False,
            'message': 'User not found'
        }), 404
    
    # Get updated user
    user = users_collection.find_one({'_id': ObjectId(user_id)})
    
    return jsonify({
        'success': True,
        'data': serialize_doc(user),
        'message': 'User updated successfully'
    })

# DELETE user
@app.route('/api/users/<string:user_id>', methods=['DELETE'])
def delete_user(user_id):
    # Find user first
    user = users_collection.find_one({'_id': ObjectId(user_id)})
    
    if not user:
        return jsonify({
            'success': False,
            'message': 'User not found'
        }), 404
    
    # Delete user and all related content
    users_collection.delete_one({'_id': ObjectId(user_id)})
    posts_collection.delete_many({'user_id': user_id})
    
    return jsonify({
        'success': True,
        'data': serialize_doc(user),
        'message': 'User and associated content deleted successfully'
    })

# ============= POST ROUTES =============

# GET all posts
@app.route('/api/posts', methods=['GET'])
def get_posts():
    # Get sort parameter from query string (default: latest)
    sort_by = request.args.get('sort', 'latest')
    
    # Determine sort criteria
    sort_criteria = {
        'latest': ('created_at', -1),      # Newest first
        'oldest': ('created_at', 1),        # Oldest first
        'author': ('user_name', 1),         # By author name A-Z
        'content': ('content', 1),          # By content A-Z
        'comments': ('comments', -1)        # Most comments first
    }
    
    # Get the sort field and direction, default to latest if invalid
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
                'author_posts': 0  # Remove the lookup array, keep the count
            }
        }
    ]
    
    # Add sort stage based on criteria
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

# GET single post by ID
@app.route('/api/posts/<string:post_id>', methods=['GET'])
def get_post(post_id):
    # Find post by ObjectId
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

# POST create new post
@app.route('/api/posts', methods=['POST'])
def create_post():
    data = request.get_json()
    
    if not data or 'content' not in data or 'user_id' not in data:
        return jsonify({
            'success': False,
            'message': 'Content and user_id are required'
        }), 400
    
    # Verify author exists
    author = users_collection.find_one({'_id': ObjectId(data['user_id'])})
    if not author:
        return jsonify({
            'success': False,
            'message': 'User not found'
        }), 404
    
    # Create new post document with embedded comments array
    new_post = {
        'user_name': author['name'],
        'user_id': data['user_id'],
        'content': data['content'],
        'created_at': datetime.now(),
        'comments': []  # Empty comments array
    }
    
    # Insert into MongoDB
    result = posts_collection.insert_one(new_post)
    new_post['id'] = str(result.inserted_id)
    del new_post['_id']
    
    return jsonify({
        'success': True,
        'data': new_post,
        'message': 'Post created successfully'
    }), 201

# PUT update post
@app.route('/api/posts/<string:post_id>', methods=['PUT'])
def update_post(post_id):
    data = request.get_json()
    
    # Prepare update fields
    update_fields = {}
    if 'content' in data:
        update_fields['content'] = data['content']
    
    # Update in MongoDB
    result = posts_collection.update_one(
        {'_id': ObjectId(post_id)},
        {'$set': update_fields}
    )
    
    if result.matched_count == 0:
        return jsonify({
            'success': False,
            'message': 'Post not found'
        }), 404
    
    # Get updated post
    post = posts_collection.find_one({'_id': ObjectId(post_id)})
    
    return jsonify({
        'success': True,
        'data': serialize_doc(post),
        'message': 'Post updated successfully'
    })

# DELETE post
@app.route('/api/posts/<string:post_id>', methods=['DELETE'])
def delete_post(post_id):
    # Find post first
    post = posts_collection.find_one({'_id': ObjectId(post_id)})
    
    if not post:
        return jsonify({
            'success': False,
            'message': 'Post not found'
        }), 404
    
    # Delete post (comments are embedded so they're automatically deleted)
    posts_collection.delete_one({'_id': ObjectId(post_id)})
    
    return jsonify({
        'success': True,
        'data': serialize_doc(post),
        'message': 'Post deleted successfully'
    })

# ============= COMMENT ROUTES =============

# GET comments for a post
@app.route('/api/posts/<string:post_id>/comments', methods=['GET'])
def get_post_comments(post_id):
    # Find post
    post = posts_collection.find_one({'_id': ObjectId(post_id)})
    
    if not post:
        return jsonify({
            'success': False,
            'message': 'Post not found'
        }), 404
    
    # Get embedded comments
    comments = post.get('comments', [])
    
    return jsonify({
        'success': True,
        'data': comments,
        'count': len(comments)
    })

# POST create new comment (add to post's comments array)
@app.route('/api/posts/<string:post_id>/comments', methods=['POST'])
def create_comment(post_id):
    # Find post
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
    
    # Verify user exists
    user = users_collection.find_one({'_id': ObjectId(data['user_id'])})
    if not user:
        return jsonify({
            'success': False,
            'message': 'User not found'
        }), 404
    
    # Create new comment object
    new_comment = {
        'user_id': data['user_id'],
        'content': data['content']
    }
    
    # Add comment to post's comments array using $push
    posts_collection.update_one(
        {'_id': ObjectId(post_id)},
        {'$push': {'comments': new_comment}}
    )
    
    return jsonify({
        'success': True,
        'data': new_comment,
        'message': 'Comment added successfully'
    }), 201

# DELETE comment (remove from post's comments array)
@app.route('/api/posts/<string:post_id>/comments/<int:comment_index>', methods=['DELETE'])
def delete_comment(post_id, comment_index):
    # Find post
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
    
    # Get the comment before deleting
    deleted_comment = comments[comment_index]
    
    # Remove comment from array by index
    comments.pop(comment_index)
    
    # Update post with new comments array
    posts_collection.update_one(
        {'_id': ObjectId(post_id)},
        {'$set': {'comments': comments}}
    )
    
    return jsonify({
        'success': True,
        'data': deleted_comment,
        'message': 'Comment deleted successfully'
    })

# Root route for testing
@app.route('/', methods=['GET'])
def root():
    return jsonify({
        'message': 'Blog System API with MongoDB',
        'version': '1.0',
        'database': 'MongoDB',
        'endpoints': {
            'users': '/api/users',
            'posts': '/api/posts',
            'comments': '/api/posts/:postId/comments',
            'health': '/api/health'
        }
    })

# Health check
@app.route('/api/health', methods=['GET'])
def health_check():
    # Check MongoDB connection
    try:
        client.admin.command('ping')
        db_status = 'connected'
    except Exception as e:
        db_status = f'disconnected: {str(e)}'
    
    return jsonify({
        'success': True,
        'message': 'Server is running',
        'timestamp': datetime.now().isoformat(),
        'database': db_status,
        'stats': {
            'users': users_collection.count_documents({}),
            'posts': posts_collection.count_documents({})
        }
    })

if __name__ == '__main__':
    print('Blog System API Server with MongoDB')
    print('Server is running on http://localhost:5000')
    print('\nDatabase: MongoDB (blog)')
    print('Collections: users, posts (with embedded comments)')
    print('\nData Structure:')
    print('  Users: {name, email}')
    print('  Posts: {user_name, user_id, content, created_at, comments[]}')
    print('  Comments: Embedded in posts {user_id, content}')
    print('\nAvailable endpoints:')
    print('  Users:    /api/users')
    print('  Posts:    /api/posts')
    print('  Comments: /api/posts/:postId/comments')
    print('  Health:   /api/health')
    print('\nNote: Make sure MongoDB is running on localhost:27017')
    print('Uncomment init_database() to populate with sample data')
    app.run(debug=True, host='0.0.0.0', port=5000)
