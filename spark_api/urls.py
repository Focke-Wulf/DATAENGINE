from django.urls import path
from . import views

urlpatterns = [
    path('execute', views.execute, name='execute'),
    path('log', views.log, name='log'),
    path('kill', views.kill, name='kill'),
]
