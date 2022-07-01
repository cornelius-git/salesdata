import pymysql
import pandas as pd

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String,BigInteger,Float,Text,ForeignKey,Table,MetaData
from sqlalchemy.orm import sessionmaker,Session
from sqlalchemy.orm import relationship
import datetime
metadata = MetaData()
# 创建连接引擎
# echo=True表示会用logger的方式打印传到数据库的SQL
engine = create_engine('mysql+pymysql://root:@localhost/testt?charset=utf8mb4', echo=False)

# 表格对象基类
Base = declarative_base()

cursor = Session(engine)






class shujuduihuan:
    def __init__(self):
        self.base_pd = pd.DataFrame()
        self.gh = 6
        self.log = open("record.text",mode="a+",encoding="utf-8")
    def jindong(self):
        print(self.gh)
        index = 0
        while True:
            sql = "SELECT * from jingdong LEFT JOIN guanxi on jingdong.child_number" \
                  " = guanxi.child_number WHERE jingdong.`indexs`={} and" \
                  " jingdong.business_code = guanxi.business_code".format(index)
            ress = cursor.execute(sql)

            self.base = [dict(zip(res.keys(),res)) for res in ress]

            if len(self.base) == 1:
                self.base = self.base[0]
                # 一件商品多个订单
                for i in range(0,self.base["bussiness_number"]):
                    self.business_id = self.base["business_id"]
                    print(self.business_id)
                    self.integral = self.base["price"]
                    self.business_code = self.base["business_code"]
                    self.fuyu()
            elif len(self.base)>1:
                # 同一个订单下，在福域上面多次下单
                for j in self.base:
                    self.base = j
                    # 一件商品多个订单
                    for i in range(0, self.base["bussiness_number"]):
                        self.business_id = self.base["business_id"]
                        print(self.business_id)
                        self.integral = self.base["price"]
                        self.business_code = self.base["business_code"]
                        self.fuyu()
            else:
                break
            index += 1
        self.base_pd.to_excel("新的结果3.xlsx")
        self.log.close()
        print(index)

    def fuyu(self):
        fg = self.integral
        sql = "SELECT * FROM fuyu WHERE business_id = '{}' ORDER BY get_create_time asc".format(self.business_id)
        ress = cursor.execute(sql)
        result = [dict(zip(res.keys(),res)) for res in ress]
        if not result :
            print("未查询到",self.business_id)
            self.log.write(str(self.business_id)+"\n")
        for s_dict in result:
            integral = s_dict["get_integral"]
            if fg == 0:
                return
            elif integral <= fg:
                self.base.update(s_dict)
                sql = "DELETE FROM fuyu WHERE `index` = {}".format(s_dict["index"])
                cursor.execute(sql)
                fg = fg - integral
                self.base_pd = pd.concat([self.base_pd,pd.DataFrame(self.base,index=[1])])
            elif fg < integral:
                s_dict["get_integral"] = fg
                self.base.update(s_dict)
                if integral - fg == 0:
                    sql = "DELETE FROM fuyu WHERE `index` = {}".format(s_dict["index"])
                else:
                    sql = "UPDATE fuyu set get_integral = {} WHERE `index` = {}".format(integral-fg,s_dict["index"])
                cursor.execute(sql)

                fg = 0
                self.base_pd = pd.concat([self.base_pd,pd.DataFrame(self.base,index=[1])])
                return

if __name__ == "__main__":
    s =datetime.datetime.now()
    shujuduihuan().jindong()
    print(datetime.datetime.now()-s)


