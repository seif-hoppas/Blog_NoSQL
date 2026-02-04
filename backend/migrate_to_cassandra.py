"""
MongoDB to Cassandra Migration Script
Phase 2: Data Migration

This script copies all existing data from MongoDB to Cassandra.
Run this after setting up dual writes to ensure all historical data is migrated.
"""

from datetime import datetime
from pymongo import MongoClient
from bson import ObjectId
from cassandra.cluster import Cluster
import uuid
import sys

# ============= DATABASE CONNECTIONS =============

print("Connecting to databases...")

# MongoDB Connection
mongo_client = MongoClient('mongodb://localhost:27017/')
mongo_db = mongo_client['blog']
users_collection = mongo_db['users']
posts_collection = mongo_db['posts']

# Cassandra Connection
cassandra_cluster = Cluster(['localhost'])
cassandra_session = cassandra_cluster.connect()

# ============= HELPER FUNCTIONS =============

def mongo_id_to_uuid(mongo_id):
    """Convert MongoDB ObjectId to UUID (for Cassandra)"""
    hex_str = str(mongo_id).ljust(32, '0')[:32]
    return uuid.UUID(hex_str)

# ============= CASSANDRA SETUP =============

def setup_cassandra():
    """Create Cassandra keyspace and tables if they don't exist"""
    print("\nSetting up Cassandra keyspace and tables...")
    
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
    
    # Create users_by_email table
    cassandra_session.execute("""
        CREATE TABLE IF NOT EXISTS users_by_email (
            email TEXT PRIMARY KEY,
            id UUID,
            name TEXT
        )
    """)
    
    # Create posts table
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
    
    # Create posts_by_author table
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
    
    # Create posts_by_content table
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
    
    # Create posts_by_id table
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
    
    # Create author_post_counts table
    cassandra_session.execute("""
        CREATE TABLE IF NOT EXISTS author_post_counts (
            user_id UUID PRIMARY KEY,
            post_count COUNTER
        )
    """)
    
    print("Cassandra setup complete!")

# ============= MIGRATION FUNCTIONS =============

def migrate_users():
    """Migrate all users from MongoDB to Cassandra"""
    print("\n" + "=" * 50)
    print("MIGRATING USERS")
    print("=" * 50)
    
    users = list(users_collection.find({}))
    total = len(users)
    migrated = 0
    errors = 0
    
    print(f"Found {total} users to migrate")
    
    for user in users:
        try:
            user_uuid = mongo_id_to_uuid(user['_id'])
            name = user.get('name', 'Unknown')
            email = user.get('email', '')
            
            # Insert into users table
            cassandra_session.execute("""
                INSERT INTO users (id, name, email)
                VALUES (%s, %s, %s)
            """, (user_uuid, name, email))
            
            # Insert into users_by_email table
            if email:
                cassandra_session.execute("""
                    INSERT INTO users_by_email (email, id, name)
                    VALUES (%s, %s, %s)
                """, (email, user_uuid, name))
            
            migrated += 1
            print(f"  ✓ Migrated user: {name} ({email})")
            
        except Exception as e:
            errors += 1
            print(f"  ✗ Error migrating user {user.get('name', 'Unknown')}: {e}")
    
    print(f"\nUsers migration complete: {migrated}/{total} migrated, {errors} errors")
    return migrated, errors

def migrate_posts():
    """Migrate all posts from MongoDB to Cassandra"""
    print("\n" + "=" * 50)
    print("MIGRATING POSTS")
    print("=" * 50)
    
    posts = list(posts_collection.find({}))
    total = len(posts)
    migrated = 0
    errors = 0
    
    print(f"Found {total} posts to migrate")
    
    # Track post counts per author for counter updates
    author_post_counts = {}
    
    for post in posts:
        try:
            post_uuid = mongo_id_to_uuid(post['_id'])
            user_id_str = post.get('user_id', '')
            user_uuid = mongo_id_to_uuid(user_id_str) if user_id_str else uuid.uuid4()
            user_name = post.get('user_name', 'Unknown')
            content = post.get('content', '')
            created_at = post.get('created_at', datetime.now())
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
            
            # Track author post counts
            if user_uuid not in author_post_counts:
                author_post_counts[user_uuid] = 0
            author_post_counts[user_uuid] += 1
            
            migrated += 1
            print(f"  ✓ Migrated post by {user_name}: {content[:50]}...")
            
            # Migrate comments for this post
            migrate_comments(post)
            
        except Exception as e:
            errors += 1
            print(f"  ✗ Error migrating post: {e}")
    
    # Update author post counts
    print("\nUpdating author post counts...")
    for user_uuid, count in author_post_counts.items():
        try:
            for _ in range(count):
                cassandra_session.execute("""
                    UPDATE author_post_counts SET post_count = post_count + 1 WHERE user_id = %s
                """, (user_uuid,))
            print(f"  ✓ Updated count for author {user_uuid}: {count} posts")
        except Exception as e:
            print(f"  ✗ Error updating count for {user_uuid}: {e}")
    
    print(f"\nPosts migration complete: {migrated}/{total} migrated, {errors} errors")
    return migrated, errors

def migrate_comments(post):
    """Migrate comments from a post to Cassandra"""
    comments = post.get('comments', [])
    
    if not comments:
        return 0, 0
    
    post_uuid = mongo_id_to_uuid(post['_id'])
    migrated = 0
    errors = 0
    
    for i, comment in enumerate(comments):
        try:
            comment_uuid = uuid.uuid4()
            user_id_str = comment.get('user_id', '')
            user_uuid = mongo_id_to_uuid(user_id_str) if user_id_str else uuid.uuid4()
            
            # Get user name from MongoDB
            user_name = 'Unknown'
            if user_id_str:
                try:
                    user = users_collection.find_one({'_id': ObjectId(user_id_str)})
                    if user:
                        user_name = user.get('name', 'Unknown')
                except:
                    pass
            
            content = comment.get('content', '')
            created_at = datetime.now()  # Comments don't have timestamps in MongoDB schema
            
            cassandra_session.execute("""
                INSERT INTO comments (post_id, comment_id, user_id, user_name, content, created_at)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (post_uuid, comment_uuid, user_uuid, user_name, content, created_at))
            
            migrated += 1
            print(f"    ✓ Migrated comment by {user_name}: {content[:30]}...")
            
        except Exception as e:
            errors += 1
            print(f"    ✗ Error migrating comment: {e}")
    
    return migrated, errors

def verify_migration():
    """Verify migration by comparing counts"""
    print("\n" + "=" * 50)
    print("VERIFICATION")
    print("=" * 50)
    
    # MongoDB counts
    mongo_users = users_collection.count_documents({})
    mongo_posts = posts_collection.count_documents({})
    
    # Cassandra counts
    cassandra_users = cassandra_session.execute("SELECT COUNT(*) FROM users").one()[0]
    cassandra_posts = cassandra_session.execute("SELECT COUNT(*) FROM posts_by_id").one()[0]
    
    print(f"\nMongoDB:")
    print(f"  Users: {mongo_users}")
    print(f"  Posts: {mongo_posts}")
    
    print(f"\nCassandra:")
    print(f"  Users: {cassandra_users}")
    print(f"  Posts: {cassandra_posts}")
    
    users_match = mongo_users == cassandra_users
    posts_match = mongo_posts == cassandra_posts
    
    print(f"\nVerification:")
    print(f"  Users match: {'✓ YES' if users_match else '✗ NO'}")
    print(f"  Posts match: {'✓ YES' if posts_match else '✗ NO'}")
    
    return users_match and posts_match

def clear_cassandra_data():
    """Clear all data from Cassandra (use before re-migration)"""
    print("\nClearing existing Cassandra data...")
    
    tables = ['users', 'users_by_email', 'posts', 'posts_by_author', 
              'posts_by_content', 'posts_by_id', 'comments']
    
    for table in tables:
        try:
            cassandra_session.execute(f"TRUNCATE {table}")
            print(f"  ✓ Cleared {table}")
        except Exception as e:
            print(f"  ✗ Error clearing {table}: {e}")
    
    # Counter tables can't be truncated, need to drop and recreate
    try:
        cassandra_session.execute("DROP TABLE IF EXISTS author_post_counts")
        cassandra_session.execute("""
            CREATE TABLE author_post_counts (
                user_id UUID PRIMARY KEY,
                post_count COUNTER
            )
        """)
        print("  ✓ Recreated author_post_counts")
    except Exception as e:
        print(f"  ✗ Error with author_post_counts: {e}")

def run_migration(clear_first=False):
    """Run the full migration"""
    print("\n" + "=" * 60)
    print("MONGODB TO CASSANDRA MIGRATION")
    print("=" * 60)
    print(f"Started at: {datetime.now().isoformat()}")
    
    # Setup Cassandra
    setup_cassandra()
    
    # Optionally clear existing data
    if clear_first:
        clear_cassandra_data()
    
    # Migrate data
    users_migrated, users_errors = migrate_users()
    posts_migrated, posts_errors = migrate_posts()
    
    # Verify
    verified = verify_migration()
    
    # Summary
    print("\n" + "=" * 60)
    print("MIGRATION SUMMARY")
    print("=" * 60)
    print(f"Users: {users_migrated} migrated, {users_errors} errors")
    print(f"Posts: {posts_migrated} migrated, {posts_errors} errors")
    print(f"Verification: {'PASSED' if verified else 'FAILED'}")
    print(f"Completed at: {datetime.now().isoformat()}")
    print("=" * 60)
    
    return verified

if __name__ == '__main__':
    # Check for --clear flag
    clear_first = '--clear' in sys.argv
    
    if clear_first:
        print("\n⚠️  WARNING: This will clear all existing Cassandra data!")
        confirm = input("Type 'yes' to confirm: ")
        if confirm.lower() != 'yes':
            print("Migration cancelled.")
            sys.exit(0)
    
    success = run_migration(clear_first)
    
    if success:
        print("\n✓ Migration completed successfully!")
        print("\nNext steps:")
        print("1. Switch to app_dual_write.py to use dual writes")
        print("2. Run app_cassandra_read.py to start reading from Cassandra")
        print("3. Finally, use app_cassandra.py for Cassandra-only mode")
    else:
        print("\n✗ Migration completed with errors. Please review the logs.")
    
    sys.exit(0 if success else 1)
