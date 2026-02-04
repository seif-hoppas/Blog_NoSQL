# MongoDB to Cassandra Migration Guide

This guide documents the migration strategy from MongoDB to Apache Cassandra for the Blog System.

## Overview

The migration follows a 4-phase approach to ensure zero downtime and data consistency:

1. **Phase 1: Dual Write** - Write to both databases
2. **Phase 2: Data Migration** - Copy existing data to Cassandra
3. **Phase 3: Read Migration** - Switch reads to Cassandra
4. **Phase 4: Cleanup** - Remove MongoDB completely

## Architecture

### MongoDB Schema (Before)
```
Database: blog
├── users
│   ├── _id: ObjectId
│   ├── name: String
│   └── email: String
│
└── posts
    ├── _id: ObjectId
    ├── user_id: String
    ├── user_name: String
    ├── content: String
    ├── created_at: DateTime
    └── comments: Array
        ├── user_id: String
        └── content: String
```

### Cassandra Schema (After)
```
Keyspace: blog
├── users (id UUID PRIMARY KEY)
├── users_by_email (email TEXT PRIMARY KEY)
├── posts (created_date, created_at, id) - for date sorting
├── posts_by_author (user_id, created_at, id) - for author queries
├── posts_by_content (content_prefix, content, id) - for alphabetical sorting
├── posts_by_id (id UUID PRIMARY KEY) - for direct lookups
├── comments (post_id, created_at, comment_id)
└── author_post_counts (user_id UUID PRIMARY KEY, post_count COUNTER)
```

## Files Overview

| File | Purpose | Phase |
|------|---------|-------|
| `app_mongodb.py` | Original MongoDB-only backend | Pre-migration |
| `app_dual_write.py` | Writes to both MongoDB and Cassandra | Phase 1 |
| `migrate_to_cassandra.py` | Migration script | Phase 2 |
| `app_cassandra_read.py` | Reads from Cassandra, writes to both | Phase 3 |
| `app_cassandra.py` | Cassandra-only backend | Phase 4 |

## Migration Steps

### Prerequisites

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Ensure both databases are running:
   - MongoDB on `localhost:27017`
   - Cassandra on `localhost:9042`

### Phase 1: Enable Dual Writes

1. Stop the current server
2. Start with dual write mode:
```bash
python app_dual_write.py
```

3. All new writes will go to both MongoDB and Cassandra
4. Existing data is still only in MongoDB

### Phase 2: Migrate Existing Data

1. Run the migration script:
```bash
python migrate_to_cassandra.py
```

2. To clear and re-migrate (if needed):
```bash
python migrate_to_cassandra.py --clear
```

3. Verify migration was successful by checking the output

### Phase 3: Switch Reads to Cassandra

1. Stop the dual-write server
2. Start with Cassandra reads:
```bash
python app_cassandra_read.py
```

3. Monitor for any issues - MongoDB fallback is available
4. Check response headers for `source` field to verify reads are from Cassandra

### Phase 4: Remove MongoDB

1. Verify all reads are successful from Cassandra
2. Stop the Cassandra-read server
3. Start Cassandra-only mode:
```bash
python app_cassandra.py
```

4. MongoDB can now be safely decommissioned

## Rollback Procedures

### Rollback to Phase 3
If issues occur in Phase 4, restart `app_cassandra_read.py` - it has MongoDB fallback.

### Rollback to Phase 1
If issues occur in Phase 3, restart `app_dual_write.py` and reads will come from MongoDB.

### Rollback to MongoDB Only
If serious issues occur, restart `app_mongodb.py` - all original data is preserved.

## Data Model Decisions

### Why Multiple Tables for Posts?

Cassandra doesn't support ad-hoc queries - you query by partition key. Different access patterns require different tables:

- **posts**: Partitioned by date for "recent posts" queries
- **posts_by_author**: Partitioned by author for "user's posts" queries
- **posts_by_content**: Partitioned by first letter for alphabetical sorting
- **posts_by_id**: For direct post lookups

### Counter Tables

`author_post_counts` uses Cassandra's counter type for efficient increment/decrement operations.

### Denormalization

Unlike MongoDB, Cassandra requires denormalized data. User names are stored in posts table to avoid joins.

## Monitoring

Check health endpoint for database status:
```bash
curl http://localhost:5000/api/health
```

Response includes:
- Database connection status
- Current migration phase
- Data statistics

## Troubleshooting

### "Table does not exist" Error
Run the migration script first - it creates the Cassandra schema.

### Data Mismatch After Migration
Run migration with `--clear` flag to re-sync all data.

### Cassandra Connection Failed
1. Check if Cassandra is running
2. Verify keyspace 'blog' exists
3. Check network connectivity

## API Changes

The API remains backward compatible. New fields in responses:

- `author_post_count`: Number of posts by this author
- `commentsCount`: Number of comments on the post
- `source`: (in some endpoints) Which database served the request

## Performance Notes

- Cassandra excels at write-heavy workloads
- Reads are optimized for specific query patterns
- Counter updates are eventually consistent
- Use appropriate consistency levels in production
