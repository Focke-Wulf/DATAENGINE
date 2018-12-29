import psycopg2
from postgresql.dbException import DataBaseException
class PostgresData:
    def __init__(self,):
        self.conn =psycopg2.connect(database="guoyunlog",user="postgres",password="123",host="localhost",port="5432")
        self.cur = self.conn.cursor()

        self.valueFunctions = {
                               'create_table':self.create_table,
                                }
    
    def create_table(self,table_name,header_name,header_type):
        if len(header_name) != len(header_type):
            try:
                raise(DataBaseException("ERROR : header lenght not equal to type"))
            except DataBaseException as e:
                print(e.messages)
                return None      
        else:
            sql = "create table "+table_name+"("
            for head in range(len(header_name)):
                sql_ = header_name[head] + " "+ header_type[head] + ","
                sql = sql + sql_
            sql = sql.rstrip(',')
            sql = sql + ");"
            print(sql)
            self.cur.execute(sql)
            self.conn.commit()
            self.conn.close()

    def insert(self,table_name,header_name,header_type,sql_value,length_value):
        """
        example 
        # hn = ['a','b']
        # ht = ['int','text']
        # vl = ['1','b']
        # postsql = PostgresData()
        # postsql.insert('test',hn,ht,vl) 
        # 
        # 
        """

    
        string_type_vertification = ['varchar','text','TEXT','character','char','CHARACTER','CHAR','VARCHAR','character varying','CHARACTER VARYING']

        if len(header_name) != length_value:
            
            try:
                raise(DataBaseException("ERROR : header lenght not equal to value"))
            except DataBaseException as e:
                print(e.messages)
                return None    
        else:
           
            sql = "INSERT INTO "+table_name + " "
            sql_head = "("
           
            
            for i in range(length_value):
                sql_head =sql_head+ header_name[i] + ","
                
                # if header_type[i] in string_type_vertification:
                #     sql_value = sql_value + "'"+str(value_list[i])+"'"+ "," 
                # else:        
                
                #    sql_value = sql_value + str(value_list[i])+ ","    
            
            sql_head  = sql_head .rstrip(',')
            # sql_value = sql_value.rstrip(',')
            sql_head = sql_head + ")"
            sql_value ="VALUES " + sql_value + ";"
            sql = sql + sql_head  +' '+sql_value
            PostgresData.__init__(self)
            self.cur.execute(sql)
            self.conn.commit()
            
