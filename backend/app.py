from flask import Flask, jsonify, request
from flask_cors import CORS
from datetime import datetime

app = Flask(__name__)
CORS(app)

# Dummy data storage (replace with MongoDB later)
users = [
    {
        'id': '1',
        'username': 'johndoe',
        'email': 'john@example.com',
        'name': 'John Doe',
        'bio': 'Software developer and tech enthusiast',
        'createdAt': '2026-01-10T08:00:00Z'
    },
    {
        'id': '2',
        'username': 'janesmit',
        'email': 'jane@example.com',
        'name': 'Jane Smith',
        'bio': 'Content writer and blogger',
        'createdAt': '2026-01-12T10:30:00Z'
    },
    {
        'id': '3',
        'username': 'mikebrown',
        'email': 'mike@example.com',
        'name': 'Mike Brown',
        'bio': 'Photography and travel lover',
        'createdAt': '2026-01-14T14:15:00Z'
    }
]

posts = [
    {
        'id': '1',
        'title': 'Getting Started with Python Flask',
        'content': 'Flask is a lightweight WSGI web application framework in Python. It is designed to make getting started quick and easy, with the ability to scale up to complex applications. In this post, we will explore the basics of Flask and build a simple web application.',
        'authorId': '1',
        'author': 'johndoe',
        'createdAt': '2026-01-15T10:00:00Z',
        'updatedAt': '2026-01-15T10:00:00Z'
    },
    {
        'id': '2',
        'title': 'The Art of Content Writing',
        'content': 'Content writing is more than just putting words on a page. It requires understanding your audience, crafting compelling narratives, and delivering value. In this article, I share my top tips for creating engaging content that resonates with readers.',
        'authorId': '2',
        'author': 'janesmit',
        'createdAt': '2026-01-16T14:30:00Z',
        'updatedAt': '2026-01-16T14:30:00Z'
    },
    {
        'id': '3',
        'title': 'Travel Photography Tips',
        'content': 'Capturing the perfect travel photo requires more than just a good camera. Learn about composition, lighting, and timing to take your travel photography to the next level. This guide covers essential techniques for both beginners and experienced photographers.',
        'authorId': '3',
        'author': 'mikebrown',
        'createdAt': '2026-01-17T09:15:00Z',
        'updatedAt': '2026-01-17T09:15:00Z'
    },
    {
        'id': '4',
        'title': 'Building RESTful APIs',
        'content': 'REST APIs are the backbone of modern web applications. This tutorial walks through creating a robust API with proper error handling, authentication, and documentation. We will use Flask to build a production-ready API from scratch.',
        'authorId': '1',
        'author': 'johndoe',
        'createdAt': '2026-01-18T11:45:00Z',
        'updatedAt': '2026-01-18T11:45:00Z'
    }
]

comments = [
    {
        'id': '1',
        'postId': '1',
        'userId': '2',
        'username': 'janesmit',
        'content': 'Great introduction to Flask! Very helpful for beginners.',
        'createdAt': '2026-01-15T12:30:00Z'
    },
    {
        'id': '2',
        'postId': '1',
        'userId': '3',
        'username': 'mikebrown',
        'content': 'Thanks for sharing this. Looking forward to more Python tutorials!',
        'createdAt': '2026-01-15T15:20:00Z'
    },
    {
        'id': '3',
        'postId': '2',
        'userId': '1',
        'username': 'johndoe',
        'content': 'Excellent tips! I will definitely apply these to my blog.',
        'createdAt': '2026-01-16T16:00:00Z'
    },
    {
        'id': '4',
        'postId': '3',
        'userId': '2',
        'username': 'janesmit',
        'content': 'Beautiful photos! What camera do you use?',
        'createdAt': '2026-01-17T10:30:00Z'
    },
    {
        'id': '5',
        'postId': '3',
        'userId': '3',
        'username': 'mikebrown',
        'content': 'I use a Canon EOS R5. Great for both photos and video!',
        'createdAt': '2026-01-17T11:00:00Z'
    }
]

# ============= USER ROUTES =============

# GET all users
@app.route('/api/users', methods=['GET'])
def get_users():
    return jsonify({
        'success': True,
        'data': users,
        'count': len(users)
    })

# GET single user by ID
@app.route('/api/users/<string:user_id>', methods=['GET'])
def get_user(user_id):
    user = next((u for u in users if u['id'] == user_id), None)
    
    if not user:
        return jsonify({
            'success': False,
            'message': 'User not found'
        }), 404
    
    # Get user's posts
    user_posts = [p for p in posts if p['authorId'] == user_id]
    
    return jsonify({
        'success': True,
        'data': {
            **user,
            'postsCount': len(user_posts)
        }
    })

# POST create new user
@app.route('/api/users', methods=['POST'])
def create_user():
    data = request.get_json()
    
    if not data or 'username' not in data or 'email' not in data:
        return jsonify({
            'success': False,
            'message': 'Username and email are required'
        }), 400
    
    # Check if username or email already exists
    if any(u['username'] == data['username'] for u in users):
        return jsonify({
            'success': False,
            'message': 'Username already exists'
        }), 400
    
    if any(u['email'] == data['email'] for u in users):
        return jsonify({
            'success': False,
            'message': 'Email already exists'
        }), 400
    
    new_user = {
        'id': str(int(datetime.now().timestamp() * 1000)),
        'username': data['username'],
        'email': data['email'],
        'name': data.get('name', ''),
        'bio': data.get('bio', ''),
        'createdAt': datetime.now().isoformat() + 'Z'
    }
    
    users.append(new_user)
    
    return jsonify({
        'success': True,
        'data': new_user,
        'message': 'User created successfully'
    }), 201

# PUT update user
@app.route('/api/users/<string:user_id>', methods=['PUT'])
def update_user(user_id):
    user = next((u for u in users if u['id'] == user_id), None)
    
    if not user:
        return jsonify({
            'success': False,
            'message': 'User not found'
        }), 404
    
    data = request.get_json()
    
    if 'name' in data:
        user['name'] = data['name']
    if 'bio' in data:
        user['bio'] = data['bio']
    if 'email' in data:
        user['email'] = data['email']
    
    return jsonify({
        'success': True,
        'data': user,
        'message': 'User updated successfully'
    })

# DELETE user
@app.route('/api/users/<string:user_id>', methods=['DELETE'])
def delete_user(user_id):
    global users, posts, comments
    
    user = next((u for u in users if u['id'] == user_id), None)
    
    if not user:
        return jsonify({
            'success': False,
            'message': 'User not found'
        }), 404
    
    users = [u for u in users if u['id'] != user_id]
    posts = [p for p in posts if p['authorId'] != user_id]
    comments = [c for c in comments if c['userId'] != user_id]
    
    return jsonify({
        'success': True,
        'data': user,
        'message': 'User and associated content deleted successfully'
    })

# ============= POST ROUTES =============

# GET all posts
@app.route('/api/posts', methods=['GET'])
def get_posts():
    sorted_posts = sorted(posts, key=lambda x: x['createdAt'], reverse=True)
    return jsonify({
        'success': True,
        'data': sorted_posts,
        'count': len(sorted_posts)
    })

# GET single post by ID
@app.route('/api/posts/<string:post_id>', methods=['GET'])
def get_post(post_id):
    post = next((p for p in posts if p['id'] == post_id), None)
    
    if not post:
        return jsonify({
            'success': False,
            'message': 'Post not found'
        }), 404
    
    # Get post comments
    post_comments = [c for c in comments if c['postId'] == post_id]
    
    return jsonify({
        'success': True,
        'data': {
            **post,
            'comments': post_comments,
            'commentsCount': len(post_comments)
        }
    })

# POST create new post
@app.route('/api/posts', methods=['POST'])
def create_post():
    data = request.get_json()
    
    if not data or 'title' not in data or 'content' not in data or 'authorId' not in data:
        return jsonify({
            'success': False,
            'message': 'Title, content, and authorId are required'
        }), 400
    
    # Verify author exists
    author = next((u for u in users if u['id'] == data['authorId']), None)
    if not author:
        return jsonify({
            'success': False,
            'message': 'Author not found'
        }), 404
    
    new_post = {
        'id': str(int(datetime.now().timestamp() * 1000)),
        'title': data['title'],
        'content': data['content'],
        'authorId': data['authorId'],
        'author': author['username'],
        'createdAt': datetime.now().isoformat() + 'Z',
        'updatedAt': datetime.now().isoformat() + 'Z'
    }
    
    posts.append(new_post)
    
    return jsonify({
        'success': True,
        'data': new_post,
        'message': 'Post created successfully'
    }), 201

# PUT update post
@app.route('/api/posts/<string:post_id>', methods=['PUT'])
def update_post(post_id):
    post = next((p for p in posts if p['id'] == post_id), None)
    
    if not post:
        return jsonify({
            'success': False,
            'message': 'Post not found'
        }), 404
    
    data = request.get_json()
    
    if 'title' in data:
        post['title'] = data['title']
    if 'content' in data:
        post['content'] = data['content']
    
    post['updatedAt'] = datetime.now().isoformat() + 'Z'
    
    return jsonify({
        'success': True,
        'data': post,
        'message': 'Post updated successfully'
    })

# DELETE post
@app.route('/api/posts/<string:post_id>', methods=['DELETE'])
def delete_post(post_id):
    global posts, comments
    
    post = next((p for p in posts if p['id'] == post_id), None)
    
    if not post:
        return jsonify({
            'success': False,
            'message': 'Post not found'
        }), 404
    
    posts = [p for p in posts if p['id'] != post_id]
    comments = [c for c in comments if c['postId'] != post_id]
    
    return jsonify({
        'success': True,
        'data': post,
        'message': 'Post and associated comments deleted successfully'
    })

# ============= COMMENT ROUTES =============

# GET comments for a post
@app.route('/api/posts/<string:post_id>/comments', methods=['GET'])
def get_post_comments(post_id):
    post = next((p for p in posts if p['id'] == post_id), None)
    
    if not post:
        return jsonify({
            'success': False,
            'message': 'Post not found'
        }), 404
    
    post_comments = [c for c in comments if c['postId'] == post_id]
    sorted_comments = sorted(post_comments, key=lambda x: x['createdAt'])
    
    return jsonify({
        'success': True,
        'data': sorted_comments,
        'count': len(sorted_comments)
    })

# POST create new comment
@app.route('/api/posts/<string:post_id>/comments', methods=['POST'])
def create_comment(post_id):
    post = next((p for p in posts if p['id'] == post_id), None)
    
    if not post:
        return jsonify({
            'success': False,
            'message': 'Post not found'
        }), 404
    
    data = request.get_json()
    
    if not data or 'content' not in data or 'userId' not in data:
        return jsonify({
            'success': False,
            'message': 'Content and userId are required'
        }), 400
    
    # Verify user exists
    user = next((u for u in users if u['id'] == data['userId']), None)
    if not user:
        return jsonify({
            'success': False,
            'message': 'User not found'
        }), 404
    
    new_comment = {
        'id': str(int(datetime.now().timestamp() * 1000)),
        'postId': post_id,
        'userId': data['userId'],
        'username': user['username'],
        'content': data['content'],
        'createdAt': datetime.now().isoformat() + 'Z'
    }
    
    comments.append(new_comment)
    
    return jsonify({
        'success': True,
        'data': new_comment,
        'message': 'Comment created successfully'
    }), 201

# DELETE comment
@app.route('/api/comments/<string:comment_id>', methods=['DELETE'])
def delete_comment(comment_id):
    global comments
    
    comment = next((c for c in comments if c['id'] == comment_id), None)
    
    if not comment:
        return jsonify({
            'success': False,
            'message': 'Comment not found'
        }), 404
    
    comments = [c for c in comments if c['id'] != comment_id]
    
    return jsonify({
        'success': True,
        'data': comment,
        'message': 'Comment deleted successfully'
    })

# Root route for testing
@app.route('/', methods=['GET'])
def root():
    return jsonify({
        'message': 'Blog System API',
        'version': '1.0',
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
    return jsonify({
        'success': True,
        'message': 'Server is running',
        'timestamp': datetime.now().isoformat(),
        'stats': {
            'users': len(users),
            'posts': len(posts),
            'comments': len(comments)
        }
    })

if __name__ == '__main__':
    print('Blog System API Server')
    print('Server is running on http://localhost:5000')
    print('\nAvailable endpoints:')
    print('  Users:    /api/users')
    print('  Posts:    /api/posts')
    print('  Comments: /api/posts/:postId/comments')
    print('  Health:   /api/health')
    app.run(debug=True, host='0.0.0.0', port=5000)
