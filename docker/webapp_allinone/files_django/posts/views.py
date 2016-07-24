from posts.models_cassandra import PostModel 
from django.http import HttpResponse
from django.shortcuts import render, redirect
from django.contrib.auth.decorators import login_required
import logging
logger = logging.getLogger(__name__)

def index(request):
    return render(request, 'posts/index.html')

@login_required
def list_posts(request):
    posts = PostModel.select_user_latest_with_date(request.user.username)
    #logger.info("Listed %d posts", len(posts))
    return render(request, 'posts/list.html', {'posts': posts})

@login_required
def create_post(request):
    if request.method == 'POST':
        new_content = request.POST.get('content', '')
        PostModel.create_post(request.user.username, content=new_content)
        return redirect('posts_list')
    else:
        return render(request, 'posts/create.html')
