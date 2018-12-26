from rest_framework import serializers
from spark_api.models import Execute
class ExecuteSerializer(serializers.ModelSerializer):
    id = serializers.IntegerField(read_only=True)
    class Meta:
        model = Execute
        fields = ('id','appid', 'script','job_id', 'type', 'sync','log')
        
