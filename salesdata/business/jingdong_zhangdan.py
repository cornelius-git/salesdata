
import pandas as pd
import numpy as np
from sql_setting import *
from data_preprocessing import *


class logicjiancha:
    # 对应关系逻辑检查
    def __init__(self):
        pass
    def check(self):
        err = open("错误.txt",mode="a+",encoding="utf-8")
        sql = "	SELECT max(`index`) FROM jingdong"
        max_value = cursor.execute(sql).first()
        for i in range(0,int(max_value[0])+1):
            sql  = "SELECT child_number,business_code FROM jingdong WHERE `index` ={} ".format(i)
            child_number,business_code = cursor.execute(sql).first()
            sql = "SELECT * from guanxi WHERE child_number={} and business_code={}".format(child_number,business_code)
            result = cursor.execute(sql).first()
            if result:
                continue
            else:
                err.write("子订单号："+str(child_number)+"\t"+"京东账单商品编号："+str(business_code)+"\n")
        err.close()


class jingdongAccount:
    def __init__(self):
        self.base_pd = pd.DataFrame()
        self.log = open("record.text",mode="a+",encoding="utf-8")
        self.erroinfo = open("errrorder.txt",mode="a+",encoding="utf-8")

    def jindong(self,result_name):
        index = 0
        while True:
            print(index)
            sql = "SELECT * from jingdong LEFT JOIN guanxi on jingdong.child_number" \
                  " = guanxi.child_number WHERE jingdong.`index`={} and" \
                  " jingdong.business_code = guanxi.business_code".format(index)
            ress = cursor.execute(sql)
            # 按福域的实际商品数量进行逐一扣减
            self.base = [dict(zip(res.keys(),res)) for res in ress]

            if len(self.base) == 1:
                self.base = self.base[0]
                # 一件商品多个订单

                for i in range(0,int(self.base["business_number"])):
                    self.business_id = "= '" + str(self.base["business_id"]).replace(" ","").replace("\t","")+"'"
                    print(self.business_id)
                    self.integral = self.base["business_price"]
                    self.business_code = self.base["business_code"]
                    self.fuyu()
            elif len(self.base)>1:
                # 同一个订单下，在福域上面多次下单
                self.business_id = []
                for j in self.base:
                    self.business_id.append(str(j["business_id"]).replace(" ","").replace("\t",""))
                self.business_id = "in " + str(tuple(self.business_id))

                self.base = self.base[0]

                # 一件商品多个订单
                for i in range(0, int(self.base["business_number"])):
                    print(self.base["child_number"],self.business_id)
                    self.integral = self.base["business_price"]
                    self.business_code = self.base["business_code"]
                    self.fuyu()

            else:
                break
            index += 1
        self.base_pd.to_excel("result\\{}.xlsx".format(result_name))
        self.base_pd.to_sql("jingdong_result",engine,if_exists="replace")
        self.log.close()

    def fuyu(self):
        fg = int(self.integral)
        sql = "SELECT * FROM fuyu_copy WHERE business_id {} ORDER BY get_create_time asc".\
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
            self.erroinfo.write("子订单号："+str(self.base["child_number"])+
                                "\t"+"福域订单号："+str(self.business_id)+"\t"+"错误积积分数"+str(fg)+"\n")

        if t_index:
            sql = 'DELETE FROM fuyu_copy WHERE `index` in ' + str(t_index).replace("[", "(").replace("]", ")")
            cursor.execute(sql)
            cursor.commit()

    def run(self):
        sql1 = "DROP TABLE IF EXISTS fuyu_copy"
        sql2 = "create table fuyu_copy select * from fuyu"
        cursor.execute(sql1)
        cursor.execute(sql2)
        cursor.commit()
        jingdong_import(file_path="D:\\1何军\\京东数据\\5月",jingdong_name="20220425-20220524整理.xlsx",sheet_name="Sheet1")
        print("导入完成")
        jingdong_relationship(file_path="D:\\1何军\\京东数据",file_name="福域积分商城订单明细表（1月-5月）正确子单号.xlsx",sheet_list=["1月","2月","3月","4月","5月"])
        print("关系导入完成")
        self.jindong(result_name="2月对账")
        print("执行结束")

class accountCompare():
    def __init__(self):
        self.compare = pd.DataFrame()
    def original_povit(self):
        # 透视原始表单
        sql = "SELECT * FROM fuyu where business_id in" \
              " ( SELECT business_id from jingdong_result)"
        original = pd.read_sql(sql,engine)
        self.o_p = original.pivot_table(index=["program_id"],values=["get_integral"],aggfunc=np.sum)
        self.o_p.reset_index(inplace=True)
        self.o_p.rename(columns={"get_integral":"start_integral"},inplace=True)
    def jindong_povit(self):
        # 透视京东表单
        sql = "select * from jingdong_result"
        jingdong= pd.read_sql(sql,engine)
        self.j_p = jingdong.pivot_table(index=["program_id"],values=["get_integral"],aggfunc=np.sum)
        self.j_p.reset_index(inplace=True)
        self.j_p.rename(columns={"get_integral":"jingdong_integral"},inplace=True)

    def run(self):
        self.original_povit()
        self.jindong_povit()
        self.result = pd.merge(self.o_p,self.j_p,how="left",on="program_id")
        self.result["chaizhi"] = self.result["jingdong_integral"] - self.result["start_integral"]
        self.result.to_excel("result\\chaizhi.xlsx")
        if len(self.result[ self.result["chaizhi"]!=0]):
            print("请核对执行过程")
        else:
            print("program_id对比未发现异常")



if __name__ == "__main__":
    # logicjiancha().check()
    jingdongAccount().run()
    accountCompare().run()


