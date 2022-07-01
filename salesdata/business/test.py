import time

import pandas as pd
import numpy as np
from sql_setting import *
from data_preprocessing import *


class jingdongAccount:
    def __init__(self):
        self.base_pd = pd.DataFrame()
        self.log = open("record.text",mode="a+",encoding="utf-8")

    def jindong(self,result_name):
        index = 0
        while True:
            sql = "SELECT * from jingdong LEFT JOIN guanxi on jingdong.child_number" \
                  " = guanxi.child_number WHERE jingdong.`index`={} and" \
                  " jingdong.business_code = guanxi.business_code".format(index)
            ress = cursor.execute(sql)

            self.base = [dict(zip(res.keys(),res)) for res in ress]

            if len(self.base) == 1:
                self.base = self.base[0]
                # 一件商品多个订单
                for i in range(0,self.base["bussiness_number"]):
                    self.business_id = self.base["business_id"]
                    print(self.business_id)
                    self.integral = self.base["business_price"]
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
                        self.integral = self.base["business_price"]
                        self.business_code = self.base["business_code"]
                        self.fuyu()
            else:
                break
            index += 1
        self.base_pd.to_excel("{}.xlsx".format(result_name))
        self.base_pd.to_sql("jingdong_result",engine,if_exists="replace")
        self.log.close()

    def fuyu(self):
        fg = int(self.integral)
        print(self.business_id)
        sql = "SELECT * FROM fuyu_copy1 WHERE business_id = '{}' ORDER BY get_create_time asc".format(self.business_id.strip(" "))
        ress = cursor.execute(sql)
        result = [dict(zip(res.keys(),res)) for res in ress]
        if not result :
            print("未查询到",self.business_id)
            self.log.write(str(self.business_id)+"\n")
            return
        t_index = []
        for s_dict in result:
            integral = int(s_dict["get_integral"])
            if fg == 0:
                return
            elif integral <= fg:
                self.base.update(s_dict)
                t_index.append(s_dict["index"])
                fg = fg - integral
                self.base_pd = pd.concat([self.base_pd, pd.DataFrame(self.base, index=[1])])
            elif fg < integral:
                print(t_index)
                if t_index:
                    print(t_index)
                    sql = 'DELETE FROM fuyu_copy1 WHERE `index` in ' + str(t_index).replace("[","(").replace("]",")")
                    cursor.execute(sql)
                    cursor.commit()

                s_dict["get_integral"] = fg
                self.base.update(s_dict)
                if integral - fg == 0:
                    sql2 = "DELETE FROM fuyu_copy1 WHERE `index` = {}".format(s_dict["index"])
                else:
                    sql2 = "UPDATE fuyu_copy1 set get_integral = {} WHERE `index` = {}".format(integral-fg,s_dict["index"])
                cursor.execute(sql2)
                cursor.commit()
                fg = 0
                self.base_pd = pd.concat([self.base_pd, pd.DataFrame(self.base, index=[1])])
                return
        if t_index:
            print(t_index)
            sql = 'DELETE FROM fuyu_copy1 WHERE `index` in ' + str(t_index).replace("[","(").replace("]",")")
            cursor.execute(sql)
            cursor.commit()
    def run(self):
        sql1 = "DROP TABLE IF EXISTS fuyu_copy1"
        sql2 = "create table fuyu_copy1 select * from fuyu"
        cursor.execute(sql1)
        cursor.execute(sql2)
        cursor.commit()
        # jingdong_import(file_path="D:\\1何军\\京东数据\\1-3月\\原始表",jingdong_name="2022.2.25-3.24.xlsx",sheet_name="Sheet1")
        print("导入完成")
        # jingdong_relationship(file_path="D:\\1何军\\京东数据",file_name="福域积分商城订单明细表（1月-5月）正确子单号.xlsx",sheet_list=["1月","2月","3月","4月","5月"])
        print("关系导入完成")
        self.jindong(result_name="1月")
        print("执行结束")

class accountCompare():
    def __init__(self):
        self.compare = pd.DataFrame()
    def original_povit(self):
        # 透视原始表单
        sql = "SELECT * FROM fuyu_copy in" \
              " ( SELECT business_id from guanxi WHERE child_number in (SELECT child_number FROM jingdong))"
        original = pd.read_sql(sql,engine)
        self.o_p = original.pivot_table(indexs=["program_id"],values=["get_integral"],aggfunc=np.sum)
        self.o_p.reset_index(inplace=True)
    def jindong_povit(self):
        # 透视京东表单
        sql = "select * from jingdong_result"
        jingdong= pd.read_sql(sql,engine)
        self.j_p = jingdong.pivot_table(indexs=["program_id"],values=["get_integral"],aggfunc=np.sum)
        self.j_p.reset_index(inplace=True)
    def run(self):
        self.original_povit()
        self.jindong_povit()
        self.result = pd.merge(self.o_p,self.j_p,how="left",on="program_id")



if __name__ == "__main__":
    now1 = datetime.datetime.now()
    for i in range(0,100):
        sql = "SELECT business_id FROM fuyu WHERE `INDEX` = {}".format(i)
        re = cursor.execute(sql)
        print(re)
    print(datetime.datetime.now()-now1)



