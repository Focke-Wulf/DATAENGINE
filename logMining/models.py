from django.db import models

# Create your models here.
class Logrecord(models.Model):
    date = models.DateTimeField(auto_now_add=True)
    filename = models.CharField(max_length=100,blank=False)
    file_loc = models.CharField(max_length=100,blank=False)
    parsed = models.BooleanField(blank=False,default=False)
    class Meta:
        ordering = ('date',)
