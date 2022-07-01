from sql_setting import *
import pandas as pd

class sibada:
    def __init__(self):
        self.base_pd = pd.DataFrame()
    def sibada_get(self):
        sibada = pd.read_excel(r"D:\1何军\财务系统\系统导出数据\斯巴达用户.xlsx")
        sibada.to_sql("sibada_get", engine, if_exists="replace", index=False)
    def data_process(self):
        # 数据预处理
        sql1 = "DROP TABLE IF EXISTS sibada_del,sibada_get_copy"
        sql2 = "CREATE TABLE sibada_get_copy SELECT * FROM sibada_get"
        cursor.execute(sql1)
        cursor.execute(sql2)
        cursor.commit()
        sql = "select sub_log_id,user_id,integral sale_integral from user_account_integral_del WHERE action_id= 105 ORDER BY create_time ASC"
        ress = cursor.execute(sql)
        results = [dict(zip(res.keys(), res)) for res in ress]
        for result in results:
            self.fubi_del(base_result=result)
        self.base_pd.to_sql("sibada_del",engine,if_exists="replace",index=False)


    def fubi_del(self,base_result):
        # 执行扣减行为
        fg = base_result["sale_integral"]
        sql = "SELECT * FROM sibada_get_copy WHERE user_id={}  ORDER BY get_create_time asc".format(base_result["user_id"])
        ress = cursor.execute(sql)
        result = [dict(zip(res.keys(),res)) for res in ress]
        if not result:
            print("未查询到", self.business_id)
        for s_dict in result:
            integral = s_dict["get_integral"]
            if fg == 0:
                return
            elif integral <= fg:
                base_result.update(s_dict)
                sql = "DELETE FROM sibada_get_copy WHERE `sibada_index` = {}".format(s_dict["sibada_index"])
                cursor.execute(sql)
                fg = fg - integral
                self.base_pd = pd.concat([self.base_pd, pd.DataFrame(base_result, index=[1])])
            elif fg < integral:
                s_dict["get_integral"] = fg
                base_result.update(s_dict)
                if integral - fg == 0:
                    sql = "DELETE FROM sibada_get_copy WHERE `sibada_index` = {}".format(s_dict["sibada_index"])
                else:
                    sql = "UPDATE sibada_get_copy set get_integral = {} WHERE `sibada_index` = {}".format(integral - fg, s_dict["sibada_index"])
                cursor.execute(sql)

                fg = 0
                self.base_pd = pd.concat([self.base_pd, pd.DataFrame(base_result, index=[1])])
                return
    def del_save(self,log_id,sub_log_id,user_id,get_integral,get_action_name,get_create_time,get_month):

        pass

if __name__  == "__main__":
    sibada().sibada_get()
    sibada().data_process()
