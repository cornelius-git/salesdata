
import pandas as pd
import numpy as np
from sql_setting import *
from data_preprocessing import *


class logicjiancha:
    # 对应关系逻辑检查
    def __init__(self):
        pass
class fuyuAccount:
    def __init__(self):
        self.base_pd = pd.DataFrame()
        self.log = open("record.text",mode="a+",encoding="utf-8")
        self.erroinfo = open("errrorder.txt",mode="a+",encoding="utf-8")

    def shop_info(self,result_name):
        index = 0
        while True:
            print(index)
            sql = "SELECT * from business_relationship where `index`={}".format(index)
            ress = cursor.execute(sql)
            # 按福域的实际商品数量进行逐一扣减
            self.base = [dict(zip(res.keys(),res)) for res in ress]
            if len(self.base) == 1:
                self.base = self.base[0]
                # 一件商品多个订单
                for i in range(0,int(self.base["business_quantity"])):
                    self.business_id = self.base["business_id"]
                    print(self.business_id)
                    self.integral = self.base["business_price"]
                    self.fuyu()
            else:
                break
            index += 1
        self.base_pd.to_excel("result\\{}.xlsx".format(result_name))
        self.base_pd.to_sql("fuyu_result",engine,if_exists="replace")
        self.log.close()

    def fuyu(self):
        fg = int(self.integral)
        sql = "SELECT * FROM fuyu_copy WHERE business_id = '{}' ORDER BY get_create_time asc".\
            format(self.business_id)
        ress = cursor.execute(sql)
        result = [dict(zip(res.keys(),res)) for res in ress]
        if not result :
            print("未查询到",self.business_id)
            self.log.write(str(self.business_id)+"\n")
        t_index = []
        for s_dict in result:
            integral = int(s_dict["get_integral"])
            if fg == 0:
                return
            elif integral <= fg:
                self.base.update(s_dict)
                # 输入到列表中统一执行
                t_index.append(s_dict["index"])
                fg = fg - integral
                self.base_pd = pd.concat([self.base_pd,pd.DataFrame(self.base,index=[1])])
            elif fg < integral:
                if t_index:
                    sql = 'DELETE FROM fuyu_copy WHERE `index` in ' + str(t_index).replace("[", "(").replace("]", ")")
                    cursor.execute(sql)
                    cursor.commit()
                s_dict["get_integral"] = fg
                self.base.update(s_dict)
                if integral - fg == 0:
                    sql = "DELETE FROM fuyu_copy WHERE `index` = {}".format(s_dict["index"])
                else:
                    sql = "UPDATE fuyu_copy set get_integral = {} WHERE `index` = {}".format(integral-fg,s_dict["index"])
                cursor.execute(sql)
                cursor.commit()
                fg = 0
                self.base_pd = pd.concat([self.base_pd,pd.DataFrame(self.base,index=[1])])
                return
        if fg != 0:
            self.erroinfo.write("福域订单号："+str(self.business_id)+"\t"+"错误积积分数"+str(fg)+"\n")

        if t_index:
            sql = 'DELETE FROM fuyu_copy WHERE `index` in ' + str(t_index).replace("[", "(").replace("]", ")")
            cursor.execute(sql)
            cursor.commit()

    def run(self):
        sql1 = "DROP TABLE IF EXISTS fuyu_copy"
        sql2 = "create table fuyu_copy select * from result"
        cursor.execute(sql1)
        cursor.execute(sql2)
        cursor.commit()
        self.shop_info(result_name="2月对账")
        print("执行结束")

class accountCompare():
    def __init__(self):
        self.compare = pd.DataFrame()
    def original_povit(self):
        # 透视原始表单
        sql = "SELECT * FROM fuyu where business_id in" \
              " ( SELECT business_id from fuyu_result)"
        original = pd.read_sql(sql,engine)
        self.o_p = original.pivot_table(index=["program_id"],values=["get_integral"],aggfunc=np.sum)
        self.o_p.reset_index(inplace=True)
        self.o_p.rename(columns={"get_integral":"start_integral"},inplace=True)
    def jindong_povit(self):
        # 透视京东表单
        sql = "select * from fuyu_result"
        jingdong= pd.read_sql(sql,engine)
        self.j_p = jingdong.pivot_table(index=["program_id"],values=["get_integral"],aggfunc=np.sum)
        self.j_p.reset_index(inplace=True)
        self.j_p.rename(columns={"get_integral":"fuyu_integral"},inplace=True)

    def run(self):
        self.original_povit()
        self.jindong_povit()
        self.result = pd.merge(self.o_p,self.j_p,how="left",on="program_id")
        self.result["chaizhi"] = self.result["fuyu_integral"] - self.result["start_integral"]
        self.result.to_excel("result\\chaizhi.xlsx")
        if len(self.result[ self.result["chaizhi"]!=0]):
            print("请核对执行过程")
        else:
            print("program_id对比未发现异常")



if __name__ == "__main__":
    # logicjiancha().check()
    fuyuAccount().run()
    accountCompare().run()


