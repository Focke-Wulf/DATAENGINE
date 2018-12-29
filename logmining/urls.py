from django.urls import path
from . import views

urlpatterns = [
    path('hello_log', views.hello_log, name='hello_log'),

]