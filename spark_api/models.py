from django.db import models
from pygments.lexers import get_all_lexers
LEXERS = [item for item in get_all_lexers() if item[1]]
LANGUAGE_CHOICES = sorted([(item[1][0], item[0]) for item in LEXERS])
# Create your models here.
class Execute(models.Model):
    created = models.DateTimeField(auto_now_add=True)
    job_id = models.CharField(max_length=500,blank=False,unique=True,primary_key=True)
    script = models.TextField()
    sync = models.IntegerField()
    log = models.TextField(blank=True)
    appid = models.TextField(blank=True)
    type = models.CharField(max_length=100,choices=LANGUAGE_CHOICES,default='python')
    class Meta:
        ordering = ('created',)
