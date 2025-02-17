import praw
from praw.models import MoreComments

class RedditClient:
    def __init__(self, user_vars) -> None:
        self._authenticate(user_vars)

    def _authenticate(self, vars):
        self.reddit = praw.Reddit(client_id=vars["client_id"],
                                  client_secret=vars["client_secret"],
                                  password=vars["password"],
                                  user_agent=vars["user_agent"],
                                  username=vars["username"]) 
        print(self.reddit.user.me())
        return
    
    # Get a list of subreddits the authenticated user is subscribed to.
    def get_subscriptions(self):
        subscriptions = []

        for subreddit in self.reddit.user.subreddits():
            subscriptions.append(subreddit)

        return subscriptions
    
    # Get a list of posts from a specific subreddit.
    def get_subreddit_posts(self, subreddit, time_filter, limit, sort_type):
        print(f"https://old.reddit.com{subreddit.url}{sort_type}/?t={time_filter}&limit={str(limit)}")

        subreddit = self.reddit.subreddit(subreddit.display_name)
        posts = []

        # Get posts based on sort_type
        if sort_type == 'top':
            posts_iterator = subreddit.top(time_filter=time_filter, limit=limit)
        else:  # hot
            posts_iterator = subreddit.hot(limit=limit)

        for post in posts_iterator:
            posts.append(post)

        return posts
    
    # Get the comments for a specific post.
    def get_post_comments(self, post):
        post.comments.replace_more(limit=5)
        return post.comments.list()
