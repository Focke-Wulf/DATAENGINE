from django.db import models

# Create your models here.
class Logrecord(models.Model):
    date = models.DateTimeField(auto_now_add=True)
    fileid = models.CharField(max_length=300,blank=False,unique=True,primary_key=True,default=False)
    filename = models.CharField(max_length=100,blank=False)
    file_loc = models.CharField(max_length=100,blank=False)
    parsed = models.BooleanField(blank=False,default=False)
    class Meta:
        ordering = ('date',)
