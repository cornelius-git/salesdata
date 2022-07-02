from data_preprocessing import *
import pymysql
import pandas as pd
from sql_setting import *
import numpy as np

class erroData:
    def __init__(self,start_time,end_time):
        # 设定筛选时间区间
        self.aim = pd.DataFrame()
        self.start_time = start_time
        self.end_time = end_time
    def exceptional(self):
        # 打赏数据处理
        # 打赏未处理部分消耗的情况
        sql = 'SELECT ul.log_id,ul.user_id,ul.integral sale_integral,ul.action_id,ul.action_name,ul.business_id,' \
              'ul.create_time sale_create_time,ul.phone,ds.log_id get_log_id,ds.integral get_integral,' \
              'ds.action_id get_action_id,ds.action_name get_action_name,ds.create_time get_create_time, ' \
              'DATE_FORMAT(ul.create_time,"%%Y-%%m") sale_month,DATE_FORMAT(ds.create_time,"%%Y-%%m") get_month FROM ' \
              'user_account_integral_log ul RIGHT JOIN user_account_integral_del ud ON ul.log_id = ud.sub_log_id ' \
              'INNER JOIN dashang ds ON ud.log_id = ds.h_log_id WHERE ul.action_id = 40 AND ul.create_time ' \
              'BETWEEN "{}" and "{}"'.format(self.start_time,self.end_time)
        self.dashang = pd.read_sql(sql,engine)
    def sibada(self):
        # 斯巴达数据匹配
        sql = " SELECT ul.log_id, ul.user_id,ul.integral sale_integral,ul.action_id,ul.action_name,ul.business_id," \
              "ul.create_time sale_create_time,ul.phone,sd.log_id get_log_id,sd.get_integral,sd.get_action_id," \
              "sd.get_action_name,sd.get_create_time, DATE_FORMAT(ul.create_time,'%%Y-%%m')" \
              "sale_month,DATE_FORMAT(sd.get_create_time,'%%Y-%%m') get_month FROM user_account_integral_log ul " \
              "INNER JOIN sibada_del sd ON ul.log_id = sd.sub_log_id WHERE ul.action_id = 40 AND ul.create_time " \
              "BETWEEN '{}' and '{}'".format(self.start_time,self.end_time)
        self.sibada_data = pd.read_sql(sql,engine)

    def refund(self):
        # 部分退款
        sql ="SELECT business_id,integral fund_integral FROM user_account_integral_log WHERE action_id =41 "
             # " AND create_time "\ "BETWEEN '{}' and '{}'".format(self.start_time,self.end_time)
        self.part = pd.read_sql(sql,engine)
        self.part["business_id"] = self.part["business_id"].apply(lambda x: str(x).split("_")[0])
        self.pivot_table = self.part.pivot_table(index=["business_id"],values=["fund_integral"],aggfunc=np.sum)
    def check(self):
        sql = 'SELECT ul.log_id,ul.user_id,ul.integral sale_integral,ul.action_id,ul.action_name,ul.business_id,' \
              'ul.create_time sale_create_time,ul.phone,ud.log_id get_log_id,ud.integral get_integral,' \
              'ud.action_id get_action_id,ud.action_name get_action_name,ud.create_time get_create_time,' \
              'DATE_FORMAT(ul.create_time,"%%Y-%%m") sale_month,DATE_FORMAT(ud.create_time,"%%Y-%%m") get_month ' \
              'FROM user_account_integral_log ul RIGHT JOIN user_account_integral_del ' \
              'ud ON ul.log_id = ud.sub_log_id WHERE ul.action_id = 40 AND ud.action_id NOT IN (78, 79, 105) AND ul.create_time ' \
              'BETWEEN "{}" and "{}"'.format(self.start_time,self.end_time)
        self.base = pd.read_sql(sql,engine)
        self.base = pd.concat([self.base,self.dashang,self.sibada_data],axis=0)
        sql = "SELECT business_id,business_channel FROM business_relationship"
        # 匹配京东还是自营
        # action_program = pd.read_sql(sql,engine)
        # self.base = pd.merge(self.base,action_program,how="left",on="business_id")
        system_value = self.base.pivot_table(index=["get_action_id"],values=["get_integral"],aggfunc=np.sum)


        self.base["business_id"] = self.base["business_id"].apply(lambda x: str(x).split("_")[0])
        # 计算实际消费
        self.pivot_table.reset_index(inplace=True)
        self.base = pd.merge(self.base,self.pivot_table,how="left",on="business_id")
        self.pivot_table.to_excel("result\\部分退款.xlsx")
        self.base.fillna(0,inplace=True)
        self.base["rel_integral"] = self.base["sale_integral"] + self.base["fund_integral"]
        # 数据校验
        self.check_original_data = self.base[["business_id","rel_integral"]].drop_duplicates()
        self.check_pivot = self.base.pivot_table(index=["business_id"], values=["get_integral"], aggfunc=np.sum)
        self.check_pivot.reset_index(inplace=True)

        self.check_result = pd.merge(self.check_original_data,self.check_pivot,how="left",on="business_id")
        self.check_result["chazhi"] = self.check_result["rel_integral"] + self.check_result["get_integral"]

        self.check_result.to_excel("result\\计算差异值.xlsx")
        if len(self.check_result[self.check_result["chazhi"] != 0]):
            print(self.check_result["business_id"])
        else:
            self.base.reset_index(drop=True,inplace=True)
            self.base.to_sql("result",engine,if_exists="replace")
            self.base.to_excel("result\\结果表.xlsx")
    def new_match(self):
        # 匹配商品属性和pid
        try:
            sql = "ALTER TABLE result ADD (program_id VARCHAR ( 255 ),integral_judge VARCHAR ( 255 ))"
            cursor.execute(sql)
            cursor.commit()
        except Exception as e:
            print(e)
        sql = "SELECT MAX(`index`) FROM result "
        max_index = cursor.execute(sql).first()[0]
        h = 0
        for i in range(0, max_index+1):
            sql = "SELECT get_action_id,get_month,business_id FROM result WHERE `index` = '{}' ".format(i)
            get_action_id, get_month, business_id = cursor.execute(sql).first()
            sql = 'SELECT business_channel FROM business_relationship WHERE business_id = "{}"'.format(business_id)
            business_channel = cursor.execute(sql).first()[0]
            sql = 'SELECT program_id,integral_judge FROM action_program WHERE action_id="{}" and start_month <= "{}"' \
                  ' and "{}" <= end_month'.format(get_action_id, get_month, get_month)

            program_id,integral_judge  = cursor.execute(sql).first()
            if program_id:
                sql = 'UPDATE result set program_id="{}",integral_judge="{}", business_channel="{}"' \
                      ' WHERE `index`={}'.format(program_id, integral_judge, business_channel, i)
                cursor.execute(sql)
            h+=1
            if h > 50000:
                cursor.commit()
                h=0
        cursor.commit()
    def macth_attribute_pid(self):
        # 匹配商品属性和pid
        try:
            sql = "ALTER TABLE result ADD (program_id VARCHAR ( 255 ),integral_judge VARCHAR ( 255 ),business_channel VARCHAR ( 255 ))"
            cursor.execute(sql)
            cursor.commit()
        except Exception as e:
            print(e)
        sql = "SELECT * FROM action_program"
        actions = cursor.execute(sql)
        action = [dict(zip(res.keys(),res)) for res in actions]
        print(action)
        for j in action:
            sql = 'UPDATE result set program_id="{}",integral_judge="{}" where get_action_id ={} and get_month >= "{}"' \
                  ' and get_month <= "{}"'.format(j["program_id"],j["integral_judge"],j["action_id"], j["start_month"], j["end_month"])
            cursor.execute(sql)
            cursor.commit()
        # 匹配京东还是自营
        # sql = "SELECT * FROM business_relationship"
        # business_relationship = cursor.execute(sql)
        # business_relationship = [dict(zip(res.keys(),res)) for res in business_relationship]
        print("项目id匹配完毕")
        # for h in business_relationship:
        #     sql = 'UPDATE result SET business_channel = "{}" WHERE business_id="{}"'.\
        #         format(h["business_channel"],h["business_id"])
        #     cursor.execute(sql)
        #     cursor.commit()



    def run(self):
        self.exceptional()
        self.sibada()
        self.refund()
        print("数据汇总完毕，正在执行检查操作")
        self.check()
        print("检查无错误，正在执行匹配工作")
        self.macth_attribute_pid()
        print("执行完毕")

class compareDifferent:
    def __init__(self):
        self.result = pd.DataFrame()

    def pov_count(self,filter_name=None,judge_word="报账",tablename=None):
        # 数据透视
        if filter_name:
            sql = "SELECT * FROM {} WHERE business_channel = '{}'".format(tablename,filter_name)
        else:
            sql = "SELECT * FROM {} ".format(tablename)

        old = pd.read_sql(sql,engine)
        old_pv = old.pivot_table(index=["integral_judge","program_id"],columns=["sale_month"],
                                      values=["get_integral"],aggfunc=np.sum)
        old_pv.reset_index(inplace=True)
        old_pv.columns = old_pv.columns.droplevel()
        columns_list = ["integral_judge","program_id"]
        for i in old_pv.columns[2:]:
            columns_list.append(str(i)+str(judge_word))
        old_pv.columns = columns_list
        old_pv.index = old_pv["program_id"]
        old_pv.drop("program_id",axis=1,inplace=True)
        return old_pv

    def month_compare(self):
        # 实现月对比输出
        self.aim = pd.DataFrame()
        self.old = self.pov_count(tablename="caiwu_account")
        self.old.drop("integral_judge",axis=1,inplace=True)
        self.new = self.pov_count(tablename="result",judge_word="新账")
        self.result = pd.merge(self.new,self.old,how="left",on="program_id")
        column_list = self.new.columns
        # print(self.result.columns)
        self.aim.index = self.new.index
        self.aim["integral_judge"] = self.new["integral_judge"]
        self.result.fillna(0,inplace=True)
        self.aim.fillna(0,inplace=True)
        for j in column_list[1:]:
            name1 = str(j).replace("新账","报账")
            self.aim[j] = self.result[j]
            try:
                # 旧账中缺少列

                self.aim[name1] = self.result[name1]
                self.aim[str(j).replace("新账","")] = self.aim[j] - self.aim[name1]

            except Exception as e:
                print(e)
                pass
        self.aim.to_excel("result\\6月对比表.xlsx")
    def bill_format(self):
        # 账单格式输出
        pass


class issueRrecord:
    # 分月统
    def __init__(self):
        pass
    def local_data(self):

        # 分月统计发放数据
        pass
    def data_compare(self):
        # 数据比对
        pass

class jingdong:
    def __init__(self):
        #
        pass


class dataSave:
    def __init__(self):
        pass
if  __name__ == "__main__":
    # erroData(start_time="2021-05-01",end_time="2022-07-01").run()
    # erroData(start_time="2021-05-01", end_time="2022-07-01").macth_attribute_pid()
    compareDifferent().month_compare()