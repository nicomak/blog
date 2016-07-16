from django.conf.urls import url

from . import views

urlpatterns = [
    url(r'^$', views.index, name='posts_index'),
    url(r'^list', views.list_posts, name='posts_list'),
    url(r'^create', views.create_post, name='posts_create'),
]
