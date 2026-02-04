# Blog System with MongoDB → Cassandra Migration

A full-stack blog application demonstrating a complete database migration strategy from MongoDB to Apache Cassandra with zero downtime.

## Features

- 📝 Create, edit, and delete blog posts
- 👥 Multiple authors and commenters
- 📰 Main feed with sorting options:
  - By date (newest/oldest)
  - By content (alphabetically)
  - By author name
  - By comment count
- 📊 Author post count displayed on each post
- 🔄 Complete migration path from MongoDB to Cassandra

## Tech Stack

- **Backend:** Python Flask
- **Frontend:** React
- **Databases:** MongoDB → Apache Cassandra

## Project Structure

```
block-system/
├── backend/
│   ├── app_mongodb.py          # Original MongoDB backend
│   ├── app_dual_write.py       # Phase 1: Dual write to both DBs
│   ├── migrate_to_cassandra.py # Phase 2: Data migration script
│   ├── app_cassandra_read.py   # Phase 3: Read from Cassandra
│   ├── app_cassandra.py        # Phase 4: Cassandra only
│   ├── requirements.txt
│   └── MIGRATION_GUIDE.md
│
└── frontend/
    ├── src/
    │   ├── App.js
    │   ├── App.css
    │   └── index.js
    └── package.json
```

## Quick Start

### Prerequisites

- Python 3.8+
- Node.js 16+
- MongoDB (for initial setup)
- Apache Cassandra (for migration)

### Backend Setup

```bash
cd backend
pip install -r requirements.txt

# Start with MongoDB only
python app_mongodb.py
```

### Frontend Setup

```bash
cd frontend
npm install
npm start
```

The app will be available at `http://localhost:3000`

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/users` | Get all users |
| POST | `/api/users` | Create user |
| GET | `/api/posts` | Get all posts |
| POST | `/api/posts` | Create post |
| PUT | `/api/posts/:id` | Update post |
| DELETE | `/api/posts/:id` | Delete post |
| GET | `/api/posts/:id/comments` | Get comments |
| POST | `/api/posts/:id/comments` | Add comment |
| GET | `/api/health` | Health check |

### Query Parameters

- `GET /api/posts?sort=latest` - Sort by newest first
- `GET /api/posts?sort=oldest` - Sort by oldest first
- `GET /api/posts?sort=content` - Sort alphabetically by content
- `GET /api/posts?sort=author` - Sort by author name
- `GET /api/posts?sort=comments` - Sort by most comments

## Migration Strategy

The migration follows a 4-phase approach for zero-downtime database migration:

### Phase 1: Dual Write
```bash
python app_dual_write.py
```
All writes go to both MongoDB and Cassandra.

### Phase 2: Data Migration
```bash
python migrate_to_cassandra.py
# Or to clear and re-migrate:
python migrate_to_cassandra.py --clear
```
Copies existing MongoDB data to Cassandra.

### Phase 3: Read Migration
```bash
python app_cassandra_read.py
```
Reads from Cassandra with MongoDB fallback.

### Phase 4: Cleanup
```bash
python app_cassandra.py
```
MongoDB completely removed, Cassandra only.

## Cassandra Data Model

```
Keyspace: blog
├── users                  - User lookup by ID
├── users_by_email         - User lookup by email
├── posts                  - Posts by date (main feed)
├── posts_by_author        - Posts by author
├── posts_by_content       - Posts sorted alphabetically
├── posts_by_id            - Direct post lookup
├── comments               - Comments by post
└── author_post_counts     - Counter table for post counts
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MONGO_URI` | `mongodb://localhost:27017/` | MongoDB connection |
| `CASSANDRA_HOST` | `localhost` | Cassandra host |

## Technologies Used

### Backend
- Python 3.x
- Flask & Flask-CORS
- PyMongo (MongoDB driver)
- cassandra-driver (Cassandra driver)

### Frontend
- React 18
- Axios
- CSS3

## License

MIT

