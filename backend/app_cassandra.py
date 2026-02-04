"""
Blog System with Cassandra Only
Phase 4 (Final): MongoDB completely removed
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
from datetime import datetime, timedelta
from cassandra.cluster import Cluster
import uuid

app = Flask(__name__)
CORS(app)

# ============= CASSANDRA CONNECTION =============

cassandra_cluster = Cluster(['localhost'])
cassandra_session = cassandra_cluster.connect('blog')

# ============= HELPER FUNCTIONS =============

def uuid_to_string(uuid_val):
    """Convert UUID to string ID"""
    return str(uuid_val).replace('-', '')[:24]

def string_to_uuid(string_id):
    """Convert string ID to UUID"""
    hex_str = string_id.ljust(32, '0')[:32]
    return uuid.UUID(hex_str)

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
    """Get comments count for a post"""
    try:
        result = cassandra_session.execute("""
            SELECT COUNT(*) as count FROM comments WHERE post_id = %s
        """, (post_uuid,))
        row = result.one()
        return row.count if row else 0
    except:
        return 0

def get_comments_for_post(post_uuid):
    """Get all comments for a post"""
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

# ============= USER ROUTES =============

@app.route('/api/users', methods=['GET'])
def get_users():
    """Get all users"""
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
        'count': len(users)
    })

@app.route('/api/users/<string:user_id>', methods=['GET'])
def get_user(user_id):
    """Get single user"""
    user_uuid = string_to_uuid(user_id)
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
        }
    })

@app.route('/api/users', methods=['POST'])
def create_user():
    """Create new user"""
    data = request.get_json()
    
    if not data or 'name' not in data or 'email' not in data:
        return jsonify({'success': False, 'message': 'Name and email are required'}), 400
    
    # Check if email exists
    result = cassandra_session.execute("""
        SELECT id FROM users_by_email WHERE email = %s
    """, (data['email'],))
    if result.one():
        return jsonify({'success': False, 'message': 'Email already exists'}), 400
    
    user_uuid = uuid.uuid4()
    
    # Insert into users table
    cassandra_session.execute("""
        INSERT INTO users (id, name, email)
        VALUES (%s, %s, %s)
    """, (user_uuid, data['name'], data['email']))
    
    # Insert into users_by_email table
    cassandra_session.execute("""
        INSERT INTO users_by_email (email, id, name)
        VALUES (%s, %s, %s)
    """, (data['email'], user_uuid, data['name']))
    
    return jsonify({
        'success': True,
        'data': {
            'id': uuid_to_string(user_uuid),
            'name': data['name'],
            'email': data['email']
        },
        'message': 'User created successfully'
    }), 201

@app.route('/api/users/<string:user_id>', methods=['PUT'])
def update_user(user_id):
    """Update user"""
    data = request.get_json()
    user_uuid = string_to_uuid(user_id)
    
    # Get current user
    result = cassandra_session.execute("""
        SELECT id, name, email FROM users WHERE id = %s
    """, (user_uuid,))
    current_user = result.one()
    
    if not current_user:
        return jsonify({'success': False, 'message': 'User not found'}), 404
    
    new_name = data.get('name', current_user.name)
    new_email = data.get('email', current_user.email)
    
    # Update users table
    cassandra_session.execute("""
        UPDATE users SET name = %s, email = %s WHERE id = %s
    """, (new_name, new_email, user_uuid))
    
    # Handle email change
    if 'email' in data and data['email'] != current_user.email:
        # Delete old email entry
        cassandra_session.execute("""
            DELETE FROM users_by_email WHERE email = %s
        """, (current_user.email,))
        
        # Insert new email entry
        cassandra_session.execute("""
            INSERT INTO users_by_email (email, id, name)
            VALUES (%s, %s, %s)
        """, (new_email, user_uuid, new_name))
    elif 'name' in data:
        # Update name in email table
        cassandra_session.execute("""
            UPDATE users_by_email SET name = %s WHERE email = %s
        """, (new_name, current_user.email))
    
    return jsonify({
        'success': True,
        'data': {
            'id': uuid_to_string(user_uuid),
            'name': new_name,
            'email': new_email
        },
        'message': 'User updated successfully'
    })

@app.route('/api/users/<string:user_id>', methods=['DELETE'])
def delete_user(user_id):
    """Delete user"""
    user_uuid = string_to_uuid(user_id)
    
    # Get user data
    result = cassandra_session.execute("""
        SELECT id, name, email FROM users WHERE id = %s
    """, (user_uuid,))
    user = result.one()
    
    if not user:
        return jsonify({'success': False, 'message': 'User not found'}), 404
    
    # Delete from users table
    cassandra_session.execute("DELETE FROM users WHERE id = %s", (user_uuid,))
    
    # Delete from users_by_email table
    cassandra_session.execute("DELETE FROM users_by_email WHERE email = %s", (user.email,))
    
    # Delete all posts by this user
    posts_result = cassandra_session.execute("""
        SELECT id, content, created_at FROM posts_by_author WHERE user_id = %s
    """, (user_uuid,))
    
    for post in posts_result:
        # Delete from all post tables
        created_date = post.created_at.strftime('%Y-%m-%d') if post.created_at else None
        content_prefix = post.content[0].upper() if post.content else 'A'
        
        if created_date:
            cassandra_session.execute("""
                DELETE FROM posts WHERE created_date = %s AND created_at = %s AND id = %s
            """, (created_date, post.created_at, post.id))
        
        cassandra_session.execute("""
            DELETE FROM posts_by_content WHERE content_prefix = %s AND content = %s AND id = %s
        """, (content_prefix, post.content, post.id))
        
        cassandra_session.execute("DELETE FROM posts_by_id WHERE id = %s", (post.id,))
        cassandra_session.execute("DELETE FROM comments WHERE post_id = %s", (post.id,))
    
    # Delete from posts_by_author
    cassandra_session.execute("DELETE FROM posts_by_author WHERE user_id = %s", (user_uuid,))
    
    return jsonify({
        'success': True,
        'data': {
            'id': uuid_to_string(user.id),
            'name': user.name,
            'email': user.email
        },
        'message': 'User and associated content deleted successfully'
    })

# ============= POST ROUTES =============

@app.route('/api/posts', methods=['GET'])
def get_posts():
    """Get all posts with sorting"""
    sort_by = request.args.get('sort', 'latest')
    posts = []
    
    if sort_by in ['latest', 'oldest']:
        # Get posts from last 30 days
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
            comments_count = get_comments_count(row.id)
            author_post_count = get_author_post_count(row.user_id)
            comments = get_comments_for_post(row.id)
            
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
            comments_count = get_comments_count(row.id)
            author_post_count = get_author_post_count(row.user_id)
            comments = get_comments_for_post(row.id)
            
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
            comments_count = get_comments_count(row.id)
            author_post_count = get_author_post_count(row.user_id)
            comments = get_comments_for_post(row.id)
            
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
        result = cassandra_session.execute("""
            SELECT id, user_id, user_name, content, created_at FROM posts_by_id
        """)
        for row in result:
            comments_count = get_comments_count(row.id)
            author_post_count = get_author_post_count(row.user_id)
            comments = get_comments_for_post(row.id)
            
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
        'sort': sort_by
    })

@app.route('/api/posts/<string:post_id>', methods=['GET'])
def get_post(post_id):
    """Get single post"""
    post_uuid = string_to_uuid(post_id)
    result = cassandra_session.execute("""
        SELECT id, user_id, user_name, content, created_at, created_date 
        FROM posts_by_id WHERE id = %s
    """, (post_uuid,))
    row = result.one()
    
    if not row:
        return jsonify({'success': False, 'message': 'Post not found'}), 404
    
    comments = get_comments_for_post(post_uuid)
    author_post_count = get_author_post_count(row.user_id)
    
    return jsonify({
        'success': True,
        'data': {
            'id': uuid_to_string(row.id),
            'user_id': uuid_to_string(row.user_id),
            'user_name': row.user_name,
            'content': row.content,
            'created_at': row.created_at.isoformat() if row.created_at else None,
            'comments': comments,
            'author_post_count': author_post_count
        }
    })

@app.route('/api/posts', methods=['POST'])
def create_post():
    """Create new post"""
    data = request.get_json()
    
    if not data or 'content' not in data or 'user_id' not in data:
        return jsonify({'success': False, 'message': 'Content and user_id are required'}), 400
    
    user_uuid = string_to_uuid(data['user_id'])
    
    # Get author
    result = cassandra_session.execute("""
        SELECT id, name FROM users WHERE id = %s
    """, (user_uuid,))
    author = result.one()
    
    if not author:
        return jsonify({'success': False, 'message': 'User not found'}), 404
    
    post_uuid = uuid.uuid4()
    created_at = datetime.now()
    created_date = created_at.strftime('%Y-%m-%d')
    content = data['content']
    content_prefix = content[0].upper() if content else 'A'
    
    # Insert into posts table (by date)
    cassandra_session.execute("""
        INSERT INTO posts (id, user_id, user_name, content, created_at, created_date)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (post_uuid, user_uuid, author.name, content, created_at, created_date))
    
    # Insert into posts_by_author table
    cassandra_session.execute("""
        INSERT INTO posts_by_author (user_id, id, user_name, content, created_at)
        VALUES (%s, %s, %s, %s, %s)
    """, (user_uuid, post_uuid, author.name, content, created_at))
    
    # Insert into posts_by_content table
    cassandra_session.execute("""
        INSERT INTO posts_by_content (content_prefix, id, user_id, user_name, content, created_at)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (content_prefix, post_uuid, user_uuid, author.name, content, created_at))
    
    # Insert into posts_by_id table
    cassandra_session.execute("""
        INSERT INTO posts_by_id (id, user_id, user_name, content, created_at, created_date)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (post_uuid, user_uuid, author.name, content, created_at, created_date))
    
    # Update author post count
    cassandra_session.execute("""
        UPDATE author_post_counts SET post_count = post_count + 1 WHERE user_id = %s
    """, (user_uuid,))
    
    return jsonify({
        'success': True,
        'data': {
            'id': uuid_to_string(post_uuid),
            'user_id': uuid_to_string(user_uuid),
            'user_name': author.name,
            'content': content,
            'created_at': created_at.isoformat(),
            'comments': []
        },
        'message': 'Post created successfully'
    }), 201

@app.route('/api/posts/<string:post_id>', methods=['PUT'])
def update_post(post_id):
    """Update post"""
    data = request.get_json()
    post_uuid = string_to_uuid(post_id)
    
    # Get current post
    result = cassandra_session.execute("""
        SELECT id, user_id, user_name, content, created_at, created_date 
        FROM posts_by_id WHERE id = %s
    """, (post_uuid,))
    current_post = result.one()
    
    if not current_post:
        return jsonify({'success': False, 'message': 'Post not found'}), 404
    
    new_content = data.get('content', current_post.content)
    
    # Update posts_by_id
    cassandra_session.execute("""
        UPDATE posts_by_id SET content = %s WHERE id = %s
    """, (new_content, post_uuid))
    
    # Note: In a production system, you'd need to handle updates to denormalized tables
    # This would require deleting old entries and inserting new ones
    
    return jsonify({
        'success': True,
        'data': {
            'id': uuid_to_string(post_uuid),
            'user_id': uuid_to_string(current_post.user_id),
            'user_name': current_post.user_name,
            'content': new_content,
            'created_at': current_post.created_at.isoformat() if current_post.created_at else None
        },
        'message': 'Post updated successfully'
    })

@app.route('/api/posts/<string:post_id>', methods=['DELETE'])
def delete_post(post_id):
    """Delete post"""
    post_uuid = string_to_uuid(post_id)
    
    # Get post data
    result = cassandra_session.execute("""
        SELECT id, user_id, user_name, content, created_at, created_date 
        FROM posts_by_id WHERE id = %s
    """, (post_uuid,))
    post = result.one()
    
    if not post:
        return jsonify({'success': False, 'message': 'Post not found'}), 404
    
    content_prefix = post.content[0].upper() if post.content else 'A'
    
    # Delete from all post tables
    cassandra_session.execute("""
        DELETE FROM posts WHERE created_date = %s AND created_at = %s AND id = %s
    """, (post.created_date, post.created_at, post_uuid))
    
    cassandra_session.execute("""
        DELETE FROM posts_by_author WHERE user_id = %s AND created_at = %s AND id = %s
    """, (post.user_id, post.created_at, post_uuid))
    
    cassandra_session.execute("""
        DELETE FROM posts_by_content WHERE content_prefix = %s AND content = %s AND id = %s
    """, (content_prefix, post.content, post_uuid))
    
    cassandra_session.execute("DELETE FROM posts_by_id WHERE id = %s", (post_uuid,))
    
    # Decrement author post count
    cassandra_session.execute("""
        UPDATE author_post_counts SET post_count = post_count - 1 WHERE user_id = %s
    """, (post.user_id,))
    
    # Delete all comments
    cassandra_session.execute("DELETE FROM comments WHERE post_id = %s", (post_uuid,))
    
    return jsonify({
        'success': True,
        'data': {
            'id': uuid_to_string(post.id),
            'user_id': uuid_to_string(post.user_id),
            'user_name': post.user_name,
            'content': post.content
        },
        'message': 'Post deleted successfully'
    })

# ============= COMMENT ROUTES =============

@app.route('/api/posts/<string:post_id>/comments', methods=['GET'])
def get_post_comments(post_id):
    """Get comments for a post"""
    post_uuid = string_to_uuid(post_id)
    comments = get_comments_for_post(post_uuid)
    
    return jsonify({
        'success': True,
        'data': comments,
        'count': len(comments)
    })

@app.route('/api/posts/<string:post_id>/comments', methods=['POST'])
def create_comment(post_id):
    """Create new comment"""
    post_uuid = string_to_uuid(post_id)
    
    # Check if post exists
    result = cassandra_session.execute("""
        SELECT id FROM posts_by_id WHERE id = %s
    """, (post_uuid,))
    if not result.one():
        return jsonify({'success': False, 'message': 'Post not found'}), 404
    
    data = request.get_json()
    
    if not data or 'content' not in data or 'user_id' not in data:
        return jsonify({'success': False, 'message': 'Content and user_id are required'}), 400
    
    user_uuid = string_to_uuid(data['user_id'])
    
    # Get user
    result = cassandra_session.execute("""
        SELECT id, name FROM users WHERE id = %s
    """, (user_uuid,))
    user = result.one()
    
    if not user:
        return jsonify({'success': False, 'message': 'User not found'}), 404
    
    comment_uuid = uuid.uuid4()
    created_at = datetime.now()
    
    cassandra_session.execute("""
        INSERT INTO comments (post_id, comment_id, user_id, user_name, content, created_at)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (post_uuid, comment_uuid, user_uuid, user.name, data['content'], created_at))
    
    return jsonify({
        'success': True,
        'data': {
            'user_id': uuid_to_string(user_uuid),
            'user_name': user.name,
            'content': data['content'],
            'created_at': created_at.isoformat()
        },
        'message': 'Comment added successfully'
    }), 201

@app.route('/api/posts/<string:post_id>/comments/<int:comment_index>', methods=['DELETE'])
def delete_comment(post_id, comment_index):
    """Delete comment by index (for compatibility with existing frontend)"""
    post_uuid = string_to_uuid(post_id)
    
    # Get all comments for the post
    result = cassandra_session.execute("""
        SELECT comment_id, user_id, user_name, content, created_at 
        FROM comments WHERE post_id = %s
    """, (post_uuid,))
    comments = list(result)
    
    if comment_index < 0 or comment_index >= len(comments):
        return jsonify({'success': False, 'message': 'Comment not found'}), 404
    
    comment = comments[comment_index]
    
    # Delete the comment
    cassandra_session.execute("""
        DELETE FROM comments WHERE post_id = %s AND created_at = %s AND comment_id = %s
    """, (post_uuid, comment.created_at, comment.comment_id))
    
    return jsonify({
        'success': True,
        'data': {
            'user_id': uuid_to_string(comment.user_id),
            'content': comment.content
        },
        'message': 'Comment deleted successfully'
    })

# ============= INFO ROUTES =============

@app.route('/', methods=['GET'])
def root():
    return jsonify({
        'message': 'Blog System API - Cassandra Only',
        'version': '4.0',
        'database': 'Apache Cassandra',
        'migration_phase': 'Phase 4: Complete (MongoDB removed)',
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
        cassandra_session.execute("SELECT now() FROM system.local")
        cassandra_status = 'connected'
    except Exception as e:
        cassandra_status = f'disconnected: {str(e)}'
    
    # Get stats
    try:
        users_count = cassandra_session.execute("SELECT COUNT(*) FROM users").one()[0]
        posts_count = cassandra_session.execute("SELECT COUNT(*) FROM posts_by_id").one()[0]
    except:
        users_count = 0
        posts_count = 0
    
    return jsonify({
        'success': True,
        'message': 'Server is running - Cassandra only mode',
        'timestamp': datetime.now().isoformat(),
        'database': {
            'cassandra': cassandra_status
        },
        'stats': {
            'users': users_count,
            'posts': posts_count
        }
    })

# ============= DATABASE INITIALIZATION =============

def init_cassandra():
    """Initialize Cassandra tables if they don't exist"""
    print("Checking Cassandra tables...")
    
    tables = ['users', 'users_by_email', 'posts', 'posts_by_author', 
              'posts_by_content', 'posts_by_id', 'comments', 'author_post_counts']
    
    for table in tables:
        try:
            cassandra_session.execute(f"SELECT * FROM {table} LIMIT 1")
            print(f"  ✓ Table {table} exists")
        except Exception as e:
            print(f"  ✗ Table {table} not found - run migrate_to_cassandra.py first!")
            raise Exception(f"Table {table} does not exist. Please run the migration script first.")

# Check tables on startup
try:
    init_cassandra()
except Exception as e:
    print(f"\n⚠️  WARNING: {e}")
    print("The server will still start, but some features may not work.")

if __name__ == '__main__':
    print('=' * 60)
    print('Blog System API Server - CASSANDRA ONLY MODE')
    print('=' * 60)
    print('Server is running on http://localhost:5000')
    print('\nDatabase: Apache Cassandra (blog keyspace)')
    print('\nMigration Phase: 4 - Complete')
    print('MongoDB has been completely removed!')
    print('\nTables:')
    print('  - users, users_by_email')
    print('  - posts, posts_by_author, posts_by_content, posts_by_id')
    print('  - comments, author_post_counts')
    print('=' * 60)
    app.run(debug=True, host='0.0.0.0', port=5000)
