import React, { useState, useEffect } from 'react';
import axios from 'axios';
import './App.css';

const API_URL = 'http://localhost:5000/api';

function App() {
  const [posts, setPosts] = useState([]);
  const [users, setUsers] = useState([]);
  const [selectedPost, setSelectedPost] = useState(null);
  const [expandedComments, setExpandedComments] = useState({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [showPostForm, setShowPostForm] = useState(false);
  const [showCommentForm, setShowCommentForm] = useState(false);
  const [editingPost, setEditingPost] = useState(null);
  const [currentUser, setCurrentUser] = useState(null);
  const [sortBy, setSortBy] = useState('latest');
  const [postFormData, setPostFormData] = useState({
    content: '',
    user_id: ''
  });
  const [commentFormData, setCommentFormData] = useState({
    content: '',
    user_id: ''
  });

  useEffect(() => {
    fetchData();
  }, [sortBy]);

  const fetchData = async () => {
    try {
      setLoading(true);
      const [postsRes, usersRes] = await Promise.all([
        axios.get(`${API_URL}/posts?sort=${sortBy}`),
        axios.get(`${API_URL}/users`)
      ]);
      setPosts(postsRes.data.data);
      setUsers(usersRes.data.data);
      if (usersRes.data.data.length > 0) {
        setCurrentUser(usersRes.data.data[0].id);
        setPostFormData(prev => ({ ...prev, user_id: usersRes.data.data[0].id }));
        setCommentFormData(prev => ({ ...prev, user_id: usersRes.data.data[0].id }));
      }
      setError(null);
    } catch (err) {
      setError('Failed to fetch data. Make sure the backend server is running.');
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  const toggleComments = (postId) => {
    setExpandedComments(prev => ({
      ...prev,
      [postId]: !prev[postId]
    }));
  };

  const handlePostSubmit = async (e) => {
    e.preventDefault();
    try {
      if (editingPost) {
        await axios.put(`${API_URL}/posts/${editingPost.id}`, postFormData);
      } else {
        await axios.post(`${API_URL}/posts`, postFormData);
      }
      setPostFormData({ content: '', user_id: currentUser });
      setShowPostForm(false);
      setEditingPost(null);
      fetchData();
    } catch (err) {
      alert('Failed to save post');
      console.error(err);
    }
  };

  const handleCommentSubmit = async (e, postId) => {
    e.preventDefault();
    try {
      await axios.post(`${API_URL}/posts/${postId}/comments`, commentFormData);
      setCommentFormData({ content: '', user_id: currentUser });
      setShowCommentForm(false);
      fetchData();
    } catch (err) {
      alert('Failed to add comment');
      console.error(err);
    }
  };

  const handleEditPost = (post) => {
    setEditingPost(post);
    setPostFormData({
      content: post.content,
      user_id: post.user_id
    });
    setShowPostForm(true);
  };

  const handleDeletePost = async (id) => {
    if (window.confirm('Are you sure you want to delete this post?')) {
      try {
        await axios.delete(`${API_URL}/posts/${id}`);
        fetchData();
      } catch (err) {
        alert('Failed to delete post');
        console.error(err);
      }
    }
  };

  const handleDeleteComment = async (postId, commentIndex) => {
    if (window.confirm('Are you sure you want to delete this comment?')) {
      try {
        await axios.delete(`${API_URL}/posts/${postId}/comments/${commentIndex}`);
        fetchData();
      } catch (err) {
        alert('Failed to delete comment');
        console.error(err);
      }
    }
  };

  const handleCancelPost = () => {
    setShowPostForm(false);
    setEditingPost(null);
    setPostFormData({ content: '', user_id: currentUser });
  };

  if (loading) {
    return <div className="container"><div className="loading">Loading blog...</div></div>;
  }

  return (
    <div className="App">
      <header className="app-header">
        <h1>📝 Blog System</h1>
        <div className="header-actions">
          <select 
            className="user-select"
            value={currentUser || ''}
            onChange={(e) => {
              setCurrentUser(e.target.value);
              setPostFormData(prev => ({ ...prev, user_id: e.target.value }));
              setCommentFormData(prev => ({ ...prev, user_id: e.target.value }));
            }}
          >
            {users.map(user => (
              <option key={user.id} value={user.id}>
                👤 {user.name} ({user.postsCount || 0} posts)
              </option>
            ))}
          </select>
          <button 
            className="btn btn-primary"
            onClick={() => {
              setShowPostForm(!showPostForm);
            }}
          >
            {showPostForm ? 'Cancel' : '+ New Post'}
          </button>
        </div>
      </header>

      <div className="container">
        {error && <div className="error-message">{error}</div>}

        <div className="main-content">
          <div className="posts-section">
            {showPostForm && (
              <div className="form-container">
                <h2>{editingPost ? 'Edit Post' : 'Create New Post'}</h2>
                <form onSubmit={handlePostSubmit}>
                  <div className="form-group">
                    <label>Title</label>
                    <input
                      type="text"
                      value={postFormData.title}
                      onChange={(e) => setPostFormData({ ...postFormData, title: e.target.value })}
                      required
                      placeholder="Enter post title"
                    />
                  </div>
                  <div className="form-group">
                    <label>Content</label>
                    <textarea
                      value={postFormData.content}
                      onChange={(e) => setPostFormData({ ...postFormData, content: e.target.value })}
                      required
                      placeholder="Write your post content..."
                      rows="8"
                    />
                  </div>
                  <div className="form-actions">
                    <button type="submit" className="btn btn-success">
                      {editingPost ? 'Update Post' : 'Publish Post'}
                    </button>
                    <button type="button" className="btn btn-secondary" onClick={handleCancelPost}>
                      Cancel
                    </button>
                  </div>
                </form>
              </div>
            )}

            <div className="posts-list">
              <div className="posts-header">
                <h2>Recent Posts</h2>
                <div className="sort-controls">
                  <label>Sort by: </label>
                  <select 
                    className="sort-select"
                    value={sortBy}
                    onChange={(e) => setSortBy(e.target.value)}
                  >
                    <option value="latest">📅 Latest</option>
                    <option value="oldest">📅 Oldest</option>
                    <option value="content">🔤 Content (A-Z)</option>
                    <option value="author">👤 Author (A-Z)</option>
                    <option value="comments">💬 Most Comments</option>
                  </select>
                </div>
              </div>
              {posts.length === 0 ? (
                <div className="empty-state">
                  <p>No posts yet. Create your first post!</p>
                </div>
              ) : (
                posts.map((post) => (
                  <div 
                    key={post.id} 
                    className={`post-card ${selectedPost && selectedPost.id === post.id ? 'selected' : ''}`}
                    onClick={() => viewPost(post)}
                  >
                    <div className="post-header">
                      <h3>{post.title || post.content?.substring(0, 50) + '...'}</h3>
                      <div className="post-author-info">
                        <span className="post-author">by @{post.user_name || post.author}</span>
                        <span className="author-posts-badge" title="Author's total posts">
                          📝 {post.author_post_count || 0} posts
                        </span>
                      </div>
                    </div>
                    <div className="post-excerpt">
                      {post.content?.substring(0, 150)}...
                    </div>
                    <div className="post-footer">
                      <div className="post-meta">
                        <small>{post.created_at ? new Date(post.created_at).toLocaleDateString() : ''}</small>
                        <span className="comments-count">💬 {post.commentsCount || post.comments?.length || 0}</span>
                      </div>
                      <div className="post-actions">
                        <button 
                          className="btn btn-sm btn-edit"
                          onClick={(e) => {
                            e.stopPropagation();
                            handleEditPost(post);
                          }}
                        >
                          Edit
                        </button>
                        <button 
                          className="btn btn-sm btn-delete"
                          onClick={(e) => {
                            e.stopPropagation();
                            handleDeletePost(post.id);
                          }}
                        >
                          Delete
                        </button>
                      </div>
                    </div>
                  </div>
                ))
              )}
            </div>
          </div>

          {selectedPost && (
            <div className="post-detail-section">
              <div className="post-detail">
                <button className="btn btn-secondary btn-back" onClick={() => setSelectedPost(null)}>
                  ← Back
                </button>
                <h1>{selectedPost.title}</h1>
                <div className="post-meta">
                  <span className="author">👤 {selectedPost.author}</span>
                  <span className="date">{new Date(selectedPost.createdAt).toLocaleString()}</span>
                </div>
                <div className="post-content">
                  {selectedPost.content}
                </div>

                <div className="comments-section">
                  <div className="comments-header">
                    <h3>Comments ({comments.length})</h3>
                    <button 
                      className="btn btn-sm btn-primary"
                      onClick={() => setShowCommentForm(!showCommentForm)}
                    >
                      {showCommentForm ? 'Cancel' : '+ Add Comment'}
                    </button>
                  </div>

                  {showCommentForm && (
                    <form className="comment-form" onSubmit={handleCommentSubmit}>
                      <textarea
                        value={commentFormData.content}
                        onChange={(e) => setCommentFormData({ ...commentFormData, content: e.target.value })}
                        required
                        placeholder="Write your comment..."
                        rows="3"
                      />
                      <div className="form-actions">
                        <button type="submit" className="btn btn-sm btn-success">
                          Post Comment
                        </button>
                        <button 
                          type="button" 
                          className="btn btn-sm btn-secondary"
                          onClick={() => {
                            setShowCommentForm(false);
                            setCommentFormData({ content: '', userId: currentUser });
                          }}
                        >
                          Cancel
                        </button>
                      </div>
                    </form>
                  )}

                  <div className="comments-list">
                    {comments.length === 0 ? (
                      <p className="no-comments">No comments yet. Be the first to comment!</p>
                    ) : (
                      comments.map((comment) => (
                        <div key={comment.id} className="comment">
                          <div className="comment-header">
                            <strong>@{comment.username}</strong>
                            <small>{new Date(comment.createdAt).toLocaleString()}</small>
                          </div>
                          <p>{comment.content}</p>
                          <button 
                            className="btn btn-sm btn-delete"
                            onClick={() => handleDeleteComment(comment.id)}
                          >
                            Delete
                          </button>
                        </div>
                      ))
                    )}
                  </div>
                </div>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

export default App;
