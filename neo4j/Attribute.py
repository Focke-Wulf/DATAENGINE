# 导入了正则表达式包
import re
# 导入了结巴分词包
import jieba
# 到入string
import string
# 导入随机
import random

#_______________________________________________________________________________
#代码规范按照PEP8标准 https://www.python.org/dev/peps/pep-0008/ 标准参考
#PEP8标准中文版本 https://blog.csdn.net/ratsniper/article/details/78954852
#本项目目的在于针对数据内容进行抽象化内容识别，详细介绍请参考项目文档
#______________________________main_____________________________________________
"""
Thanks for the beauty of math
"""
class Attribute:
    #中国人常用姓名组 （需要时可以添加）
    FNAME_LIST = ["王","张","黄","周","徐","胡","高","林","马","于","程","傅","曾",
                  "叶","余","夏","钟","田","任","方",
                 "石","熊","白","毛","江","史","候","龙","万","段","雷","钱","汤",
                  "易","常","武","赖","文", "查",
                 #无歧义姓
                 "赵", "肖", "孙", "李","吴", "郑", "冯", "陈","褚", "卫", "蒋",
                  "沈","韩", "杨", "朱", "秦",
                 "尤", "许", "何", "吕","施", "桓", "孔", "曹","严", "华", "金",
                  "魏","陶", "姜", "戚", "谢",
                 "邹", "喻", "柏", "窦","苏", "潘", "葛", "奚","范", "彭", "鲁",
                  "韦","昌", "俞", "袁", "酆",
                 "鲍", "唐", "费", "廉","岑", "薛", "贺", "倪","滕", "殷", "罗",
                  "毕","郝", "邬", "卞", "康",
                 "卜", "顾", "孟", "穆","萧", "尹", "姚", "邵","湛", "汪", "祁",
                  "禹","狄", "贝", "臧", "伏",
                 "戴", "宋", "茅", "庞","纪", "舒", "屈", "祝","董", "梁", "杜",
                  "阮","闵", "贾", "娄", "颜",
                 "郭", "邱", "骆", "蔡","樊", "凌", "霍", "虞","柯", "昝", "卢",
                  "柯","缪", "宗", "丁", "贲",
                 "邓", "郁", "杭", "洪","崔", "龚", "嵇", "邢","滑", "裴", "陆",
                  "荣","荀", "惠", "甄", "芮",
                 "羿", "储", "靳", "汲","邴", "糜", "隗", "侯","宓", "蓬", "郗",
                  "仲","栾", "钭", "历", "戎",
                 "刘", "詹", "幸", "韶","郜", "黎", "蓟", "溥","蒲", "邰", "鄂",
                  "咸","卓", "蔺", "屠", "乔",
                 "郁", "胥", "苍", "莘","翟", "谭", "贡", "劳","冉", "郦", "雍",
                  "璩","桑", "桂", "濮", "扈",
                 "冀", "浦", "庄", "晏","瞿", "阎", "慕", "茹","习", "宦", "艾",
                  "容","慎", "戈", "廖", "庾",
                 "衡", "耿", "弘", "匡","阙", "殳", "沃", "蔚","夔", "隆", "巩",
                  "聂","晁", "敖", "融", "訾",
                 "辛", "阚", "毋", "乜","鞠", "丰", "蒯", "荆","竺", "盍", "单",
                  "欧",
                 #双字姓
                 "司马", "上官", "欧阳","夏侯", "诸葛", "闻人","东方", "赫连",
                  "皇甫","尉迟", "公羊", "澹台",
                 "公冶", "宗政", "濮阳","淳于", "单于", "太叔","申屠", "公孙",
                  "仲孙","轩辕", "令狐", "徐离",
                 "宇文", "长孙", "慕容","司徒", "司空", "万俟"]

    #姓名字列表 NAME_LIST = [] / 使用newcharname.txt 作为字典

    #中国手机号码前三位
    PHONE_SERVER = [130, 131, 132, 155, 156,
                    186, 185, 186, 185, 145,
                    134, 135, 136, 137, 138,
                    139, 150, 151, 152, 157,
                    158, 182, 183, 188, 187,
                    133, 153, 180, 181, 189]
    #中国国家地区区号
    REGION_CODE = ['+86']
    #性别
    SEX_LIST = ['男','女','Male','Female']

    """
    输入数据为抽样检测好的数据        
    Cass Attribute 下所有的方法将对数据进行属性分类
    输入：数据为list形式所存储的数据格式， [[属性标签0],[数据内容....1]]
    """

    def __init__(self,Data):

        """
        :param Data: 进入的数据片段
        """
        self.data = Data


        """
        这里添加属性分析函数库命名
        """
        self.valueFunctions = {
                               'phone':self.phone_tractor,
                               'email':self.email_tractor,
                               'sex':self.sex_tractor,
                               'address':self.address_tractor,
                               'name':self.name_tractor,
                                }

#===========================这里添加属性分析函数库=================================
    def phone_tractor(self):

        #Import Data for Phone testing
        PHONECODE = self.data

        length = len(PHONECODE[1])
        counter = 0
        #粗略预判断数据的类型，如果不是数字类型则直接返回结果,
        #检测所有抽样数据类型，设置允许有错误率
        for i in range(0,length):
            num = str(PHONECODE[1][i]).replace(' ','')
            if num[:3] in Attribute.REGION_CODE and len(num[3:]) == 11 \
                    and int(num[3:6]) in Attribute.PHONE_SERVER:
                counter += 1
            elif num.isdigit() and len(num) == 11 and int(num[:3]) in\
                    Attribute.PHONE_SERVER:
                counter += 1

            else:
                self.data[0] = 'NULL'

        # 允许数据可能存在错误概率，这个概率需要调整
        ETR = counter / length
        if ETR > 0.5:
            self.data[0] = ['phone']
            return self.data
        else:
            self.data[0] = ['NULL']
            return self.data

    def email_tractor(self):
        """
        辨识邮箱的方法
        :return:
        """
        EMAILCODE = self.data

        if self.data[0] == ['NULL']:

            length = len(EMAILCODE[1])
            counter = 0

            for i in range(0,length):
                email = EMAILCODE[1][i]

                try:
                    if re.match(r'^[0-9a-zA-Z_]'
                                r'{0,19}@[0-9a-zA-Z]'
                                r'{1,13}\.[com,cn,net]{1,3}$', email):
                        counter += 1
                    else:
                        counter += 0
                except:
                    pass

            ETR = counter / length
            # 允许数据可能存在错误概率，这个概率需要调整
            if ETR > 0.5:
                self.data[0] = ['email']
                return self.data
            else:
                return self.data

        else:
            pass

    def sex_tractor(self):

        """
        辨识性别方法
        :return:
        """
        SEXCODE = self.data

        if self.data[0] == ['NULL']:

            length = len(SEXCODE[1])
            counter = 0
            for i in range(0,length):
                sex = SEXCODE[1][i]
                if sex in Attribute.SEX_LIST:
                    counter += 1
            ETR = counter / length
            if ETR > 0.9:
                self.data[0] = ['sex']
                return self.data
            else:
                return self.data

        else:
            pass

    def address_tractor(self):
        """
        建立地区文件-字典
        导入数据-分词-统计
        :return:
        """
        if self.data[0] == ['NULL']:

            place = []
            place_dict = {}
            with open ("city.txt",'rb') as fo:
                for i in fo:
                    line = i.decode('utf-8-sig')
                    newline = re.sub("[0-9]",'', line).strip()
                    place.append(newline)
            with open ("district.txt",'rb') as fo:
                for i in fo:
                    line = i.decode('utf-8-sig')
                    newline = re.sub("[0-9]",'', line).strip()
                    place.append(newline)
            with open ("province.txt",'rb') as fo:
                for i in fo:
                    line = i.decode('utf-8-sig')
                    newline = re.sub("[0-9]",'', line).strip()
                    place.append(newline)
            for i in place:
                place_dict[i] = 0


            #创建字典文件结束
            #---------------
            #读取系统

            ADDCODE = self.data
            length = len(ADDCODE[1])
            counter = 0
            def seg(seginfo):
                strData=seginfo
                seg_list = jieba.cut(strData)

                for i in seg_list:
                    if len(i) >1 :
                        for j in place_dict:

                            if i in j:
                                place_dict[j] += 1


            for i in range(0,length):

                try:
                    seg(ADDCODE[1][i])
                except:
                    pass

            for k in place_dict:
                counter += place_dict[k]

            ETR = counter/length

            if ETR >= 0.9:
                self.data[0] = ['address']
                return self.data
            else:
                return self.data
        else:
            pass

    def name_tractor(self):

        if self.data[0] == ['NULL']:
            namearray = []
            with open('newcharname.txt','rb') as fo:
                line = fo.readline().decode('utf-8-sig')
                for i in line:
                    namearray.append(i)

            NAMECODE = self.data
            length = len(NAMECODE[1])
            counter = 0
            # 中文名称具有4种
            for i in range(0,length):

                try:

                    if len(NAMECODE[1][i]) == 2:

                        if NAMECODE[1][i][0] in Attribute.FNAME_LIST and\
                                NAMECODE[1][i][1] in namearray:

                            counter += 1

                    if len(NAMECODE[1][i]) == 3:

                        if NAMECODE[1][i][0] in Attribute.FNAME_LIST \
                                and NAMECODE[1][i][1] in namearray \
                                or NAMECODE[1][i][2] in namearray :
                            counter +=1


                    if len(NAMECODE[1][i]) == 4:

                        if NAMECODE[1][i][:2] in Attribute.FNAME_LIST \
                                and NAMECODE[1][i][3] in namearray \
                                or NAMECODE[1][i][4] in namearray :
                            counter +=1

                except:
                    pass

            ETR = counter / length

            if ETR >= 0.7:
                self.data[0] = ['name']
                return self.data
            else:
                return self.data
        else:
            pass
    @staticmethod
    def ramtag(dict):
        def random_tag():
            src_digits = string.digits  # string_数字
            src_uppercase = string.ascii_uppercase  # string_大写字母
            src_lowercase = string.ascii_lowercase  # string_小写字母
            # 随机生成数字、大写字母、小写字母的组成个数（可根据实际需要进行更改）
            digits_num = random.randint(1, 6)
            uppercase_num = random.randint(1, 8 - digits_num - 1)
            lowercase_num = 8 - (digits_num + uppercase_num)
            # 生成字符串
            password = random.sample(src_digits, digits_num) + \
                       random.sample(src_uppercase, uppercase_num) + \
                       random.sample(src_lowercase, lowercase_num)
            # 打乱字符串
            random.shuffle(password)
            # 列表转字符串
            new_password = ''.join(password)
            return new_password

        DICT = dict

        compare_list = []

        for sheet_name in DICT:
            for list in DICT[sheet_name]:
                if DICT[sheet_name][list][0] == ['NULL']:
                   compare_list.append(DICT[sheet_name][list])


        for i in compare_list:
            i[0] = [random_tag()]


        return DICT




    # 添加函数判断式
    def run(self):
        self.valueFunctions['phone']()
        self.valueFunctions['email']()
        self.valueFunctions['sex']()
        self.valueFunctions['address']()
        self.valueFunctions['name']()











